package homekit

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"sync"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/srtp"
	"github.com/pion/rtp"
)

// backchannelPipeline manages the ffmpeg transcoding subprocess that converts
// incoming Opus RTP packets (from WebRTC) to AAC-ELD and sends them to the
// HomeKit camera via SRTP.
//
// Uses libfdk_aac with -latm 1 (LOAS transport, required for AAC-ELD) and
// -f latm muxer (passes through LOAS frames from the encoder). The Go side
// parses LOAS frames, extracts raw AAC access units, wraps them in RFC 3640,
// and sends via SRTP.
type backchannelPipeline struct {
	cmd      *exec.Cmd
	cancel   context.CancelFunc
	sendConn *net.UDPConn  // sends Opus RTP to ffmpeg input
	stdout   io.ReadCloser // encoder stdout (LOAS stream)
	sdpFile  string
	mu       sync.Mutex
	closed   bool
}

// findELDEncoder searches for an ffmpeg binary that can encode AAC-ELD
// using libfdk_aac.
func findELDEncoder() string {
	for _, bin := range []string{
		"/usr/local/bin/ffmpeg-homebridge",
		"/usr/local/bin/ffmpeg-fdk",
		"ffmpeg-fdk",
		"ffmpeg",
	} {
		testCmd := exec.Command(bin,
			"-hide_banner", "-loglevel", "error",
			"-f", "lavfi", "-i", "sine=frequency=440:duration=0.1",
			"-c:a", "libfdk_aac", "-profile:a", "aac_eld",
			"-ar", "16000", "-ac", "1",
			"-f", "latm", os.DevNull)
		if err := testCmd.Run(); err == nil {
			return bin
		}
	}
	return ""
}

// testELDOption tests if an ffmpeg binary supports a specific libfdk_aac option.
func testELDOption(bin string, extraArgs ...string) bool {
	args := []string{
		"-hide_banner", "-loglevel", "error",
		"-f", "lavfi", "-i", "sine=frequency=440:duration=0.1",
		"-c:a", "libfdk_aac", "-profile:a", "aac_eld",
	}
	args = append(args, extraArgs...)
	args = append(args, "-ar", "16000", "-ac", "1", "-f", "latm", os.DevNull)

	testCmd := exec.Command(bin, args...)
	return testCmd.Run() == nil
}

func startBackchannelPipeline(session *srtp.Session, sendCounter *int) (*backchannelPipeline, core.HandlerFunc, error) {
	// Open a UDP port for ffmpeg to listen on for Opus RTP input
	inputAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		return nil, nil, fmt.Errorf("backchannel: resolve: %w", err)
	}
	inputTmp, err := net.ListenUDP("udp", inputAddr)
	if err != nil {
		return nil, nil, fmt.Errorf("backchannel: listen input: %w", err)
	}
	inputPort := inputTmp.LocalAddr().(*net.UDPAddr).Port
	inputTmp.Close()

	// Create SDP file describing the Opus RTP input stream
	sdp := fmt.Sprintf(
		"v=0\r\n"+
			"o=- 0 0 IN IP4 127.0.0.1\r\n"+
			"s=backchannel\r\n"+
			"c=IN IP4 127.0.0.1\r\n"+
			"t=0 0\r\n"+
			"m=audio %d RTP/AVP 111\r\n"+
			"a=rtpmap:111 opus/48000/2\r\n",
		inputPort,
	)

	sdpFile, err := os.CreateTemp("", "go2rtc-backchannel-*.sdp")
	if err != nil {
		return nil, nil, fmt.Errorf("backchannel: create sdp: %w", err)
	}
	sdpFileName := sdpFile.Name()
	if _, err := sdpFile.WriteString(sdp); err != nil {
		sdpFile.Close()
		os.Remove(sdpFileName)
		return nil, nil, fmt.Errorf("backchannel: write sdp: %w", err)
	}
	sdpFile.Close()

	ctx, cancel := context.WithCancel(context.Background())

	pipeline := &backchannelPipeline{
		sdpFile: sdpFileName,
		cancel:  cancel,
	}

	eldEncoder := findELDEncoder()
	if eldEncoder == "" {
		cancel()
		os.Remove(sdpFileName)
		return nil, nil, fmt.Errorf("backchannel: no ffmpeg with libfdk_aac found")
	}

	if err := pipeline.startEncoder(ctx, eldEncoder, sdpFileName); err != nil {
		cancel()
		os.Remove(sdpFileName)
		return nil, nil, fmt.Errorf("backchannel: %w", err)
	}

	go pipeline.readLOASAndSend(session, sendCounter)

	// Create UDP connection to send Opus RTP to ffmpeg's input port
	ffmpegAddr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf("127.0.0.1:%d", inputPort))
	sendConn, err := net.DialUDP("udp", nil, ffmpegAddr)
	if err != nil {
		pipeline.Close()
		return nil, nil, fmt.Errorf("backchannel: dial ffmpeg: %w", err)
	}
	pipeline.sendConn = sendConn

	var seq uint16
	handler := func(packet *rtp.Packet) {
		clone := rtp.Packet{
			Header: rtp.Header{
				Version:        2,
				PayloadType:    111,
				SequenceNumber: seq,
				Timestamp:      packet.Timestamp,
				SSRC:           0x12345678,
				Marker:         packet.Marker,
			},
			Payload: packet.Payload,
		}
		seq++

		b, err := clone.Marshal()
		if err != nil {
			return
		}
		sendConn.Write(b)
	}

	go func() {
		<-ctx.Done()
		sendConn.Close()
	}()

	return pipeline, handler, nil
}

// startEncoder launches ffmpeg with libfdk_aac encoding AAC-ELD via LOAS.
//
// -latm 1 is required: without it, FDK-AAC defaults to ADTS transport which
// only supports AAC-LC. The -f latm muxer detects LOAS in the encoder output
// and passes it through unchanged.
//
// Optional flags (-eld_sbr 1, -frame_length 480) are probed and added if
// supported, to match the camera's AudioSpecificConfig.
func (p *backchannelPipeline) startEncoder(ctx context.Context, bin, sdpFile string) error {
	args := []string{
		"-hide_banner", "-loglevel", "error",
		"-protocol_whitelist", "file,rtp,udp",
		"-f", "sdp", "-i", sdpFile,
		"-c:a", "libfdk_aac", "-profile:a", "aac_eld",
		"-latm", "1",
	}
	if testELDOption(bin, "-frame_length", "480") {
		args = append(args, "-frame_length", "480")
	}
	if testELDOption(bin, "-eld_sbr", "1") {
		args = append(args, "-eld_sbr", "1")
	}
	args = append(args,
		"-ar", "16000", "-ac", "1", "-b:a", "24k",
		"-f", "latm", "pipe:1")

	cmd := exec.CommandContext(ctx, bin, args...)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("stdout pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start: %w", err)
	}

	p.cmd = cmd
	p.stdout = stdout

	go func() {
		cmd.Wait()
	}()

	return nil
}

func drainStderr(name string, stderr interface{ Read([]byte) (int, error) }) {
	if stderr == nil {
		return
	}
	scanner := bufio.NewScanner(stderr)
	for scanner.Scan() {
		log.Printf("[%s] ffmpeg: %s", name, scanner.Text())
	}
}

// readLOASAndSend reads LOAS frames from the encoder's stdout, extracts
// raw AAC-ELD frames, wraps them in RFC 3640, and sends via SRTP.
func (p *backchannelPipeline) readLOASAndSend(session *srtp.Session, sendCounter *int) {
	r := bufio.NewReaderSize(p.stdout, 4096)

	const timestampIncrement = 480
	var timestamp uint32
	var seq uint16
	var firstFrame = true

	for {
		// Find LOAS sync word: 0x56, then byte with top 3 bits = 111
		b0, err := r.ReadByte()
		if err != nil {
			return
		}
		if b0 != 0x56 {
			continue
		}

		b1, err := r.ReadByte()
		if err != nil {
			return
		}
		if b1&0xE0 != 0xE0 {
			r.UnreadByte()
			continue
		}

		b2, err := r.ReadByte()
		if err != nil {
			return
		}

		// Frame length from 13 bits: 5 bits from b1 + 8 bits from b2
		frameLen := int(b1&0x1F)<<8 | int(b2)
		if frameLen == 0 || frameLen > 8192 {
			continue
		}

		// Read AudioMuxElement
		ameBuf := make([]byte, frameLen)
		if _, err := io.ReadFull(r, ameBuf); err != nil {
			return
		}

		// Parse AudioMuxElement to extract raw AAC frame
		aacFrame := extractAACFromAME(ameBuf, firstFrame)
		firstFrame = false

		if aacFrame == nil {
			continue
		}

		// Wrap in RFC 3640 format (single AU) and send via SRTP
		payload := make([]byte, 4+len(aacFrame))
		payload[0] = 0x00
		payload[1] = 0x10 // 16 bits of AU headers
		payload[2] = byte((len(aacFrame) << 3) >> 8)
		payload[3] = byte((len(aacFrame) << 3) & 0xFF)
		copy(payload[4:], aacFrame)

		packet := &rtp.Packet{
			Header: rtp.Header{
				Version:        2,
				Marker:         true,
				SequenceNumber: seq,
				Timestamp:      timestamp,
			},
			Payload: payload,
		}
		seq++
		timestamp += timestampIncrement

		if sent, err := session.WriteRTP(packet); err == nil {
			*sendCounter += sent
		}
	}
}

// extractAACFromAME extracts the raw AAC frame from an AudioMuxElement.
// The first AME (useSameStreamMux=0) contains StreamMuxConfig and is skipped.
// Subsequent AMEs (useSameStreamMux=1) contain PayloadLengthInfo + payload.
func extractAACFromAME(data []byte, isFirst bool) []byte {
	if len(data) == 0 {
		return nil
	}

	if isFirst || data[0]&0x80 == 0 {
		// First frame or useSameStreamMux=0: contains StreamMuxConfig, skip it
		return nil
	}

	// useSameStreamMux=1: extract payload after PayloadLengthInfo
	return extractPayloadFromBitOffset(data, 1)
}

// extractPayloadFromBitOffset reads PayloadLengthInfo starting at the given
// bit offset in data, then returns the AAC payload bytes.
func extractPayloadFromBitOffset(data []byte, bitOffset int) []byte {
	payloadLen := 0
	for {
		if bitOffset+8 > len(data)*8 {
			return nil
		}
		val := readBits(data, bitOffset, 8)
		bitOffset += 8
		payloadLen += val
		if val != 255 {
			break
		}
	}

	byteOffset := bitOffset / 8
	bitRemainder := bitOffset % 8

	if bitRemainder == 0 {
		if byteOffset+payloadLen > len(data) {
			return nil
		}
		return data[byteOffset : byteOffset+payloadLen]
	}

	if byteOffset+payloadLen+1 > len(data) {
		return nil
	}
	result := make([]byte, payloadLen)
	for i := 0; i < payloadLen; i++ {
		result[i] = byte(readBits(data, bitOffset+i*8, 8))
	}
	return result
}

func readBits(data []byte, bitOffset, n int) int {
	val := 0
	for i := 0; i < n; i++ {
		byteIdx := (bitOffset + i) / 8
		bitIdx := 7 - (bitOffset+i)%8
		if byteIdx < len(data) {
			val = (val << 1) | ((int(data[byteIdx]) >> bitIdx) & 1)
		}
	}
	return val
}

// Close shuts down the ffmpeg process and cleans up resources.
func (p *backchannelPipeline) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}
	p.closed = true

	if p.cancel != nil {
		p.cancel()
	}
	if p.sendConn != nil {
		p.sendConn.Close()
	}
	if p.stdout != nil {
		p.stdout.Close()
	}
	if p.cmd != nil && p.cmd.Process != nil {
		p.cmd.Process.Kill()
	}
	if p.sdpFile != "" {
		os.Remove(p.sdpFile)
	}
	return nil
}
