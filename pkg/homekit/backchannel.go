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
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/srtp"
	"github.com/pion/rtp"
)

// backchannelPipeline manages the ffmpeg transcoding subprocess that converts
// incoming Opus RTP packets (from WebRTC) to AAC-ELD RTP packets, which are
// then sent to the HomeKit camera via SRTP.
//
// Three modes (tried in order):
//  1. RTP mode with libfdk_aac: encoder outputs RFC 3640 RTP directly to UDP.
//     No LOAS parsing needed — ffmpeg handles all framing.
//  2. Pipe mode (libfdk_aac): encoder outputs LATM to stdout, Go parses LOAS
//     frames and extracts raw AAC-ELD frames for SRTP.
//  3. RTP mode (native AAC): fallback encoder outputs RTP directly to UDP.
type backchannelPipeline struct {
	cmd      *exec.Cmd
	cancel   context.CancelFunc
	sendConn *net.UDPConn   // sends Opus RTP to ffmpeg input
	udpConn  net.PacketConn // receives AAC-ELD RTP from ffmpeg (RTP mode only)
	stdout   io.ReadCloser  // encoder stdout (pipe mode only)
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
			log.Printf("[backchannel] found ELD encoder: %s", bin)
			return bin
		}
	}
	return ""
}

// canEncodeELDViaRTP tests if an ffmpeg binary can encode AAC-ELD with
// libfdk_aac and output via RTP muxer. If withSBR is true, also tests
// with -eld_sbr 1 since that's what will be used in production.
func canEncodeELDViaRTP(bin string, withSBR bool) bool {
	listener, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		return false
	}
	defer listener.Close()
	port := listener.LocalAddr().(*net.UDPAddr).Port

	args := []string{
		"-hide_banner", "-loglevel", "error",
		"-f", "lavfi", "-i", "sine=frequency=440:duration=0.1",
		"-c:a", "libfdk_aac", "-profile:a", "aac_eld",
	}
	if withSBR {
		args = append(args, "-eld_sbr", "1")
	}
	args = append(args, "-ar", "16000", "-ac", "1",
		"-f", "rtp", fmt.Sprintf("rtp://127.0.0.1:%d", port))

	testCmd := exec.Command(bin, args...)
	if err := testCmd.Run(); err == nil {
		log.Printf("[backchannel] libfdk_aac ELD works with -f rtp (sbr=%v)", withSBR)
		return true
	}

	log.Printf("[backchannel] libfdk_aac ELD does NOT work with -f rtp (sbr=%v)", withSBR)
	return false
}

// supportsELDSBR tests if an ffmpeg binary supports -eld_sbr option.
// Tests with multiple output formats because -eld_sbr may fail with some muxers.
func supportsELDSBR(bin string) bool {
	// Try with -f null first (most compatible test)
	testCmd := exec.Command(bin,
		"-hide_banner", "-loglevel", "error",
		"-f", "lavfi", "-i", "sine=frequency=440:duration=0.1",
		"-c:a", "libfdk_aac", "-profile:a", "aac_eld", "-eld_sbr", "1",
		"-ar", "16000", "-ac", "1",
		"-f", "null", "-")
	if testCmd.Run() == nil {
		log.Printf("[backchannel] -eld_sbr support: true (null test)")
		return true
	}

	// Try with -f latm (might fail if SBR+LATM has issues)
	testCmd = exec.Command(bin,
		"-hide_banner", "-loglevel", "error",
		"-f", "lavfi", "-i", "sine=frequency=440:duration=0.1",
		"-c:a", "libfdk_aac", "-profile:a", "aac_eld", "-eld_sbr", "1",
		"-ar", "16000", "-ac", "1",
		"-f", "latm", os.DevNull)
	if testCmd.Run() == nil {
		log.Printf("[backchannel] -eld_sbr support: true (latm test)")
		return true
	}

	// Capture stderr for diagnostics
	testCmd = exec.Command(bin,
		"-hide_banner", "-loglevel", "error",
		"-f", "lavfi", "-i", "sine=frequency=440:duration=0.1",
		"-c:a", "libfdk_aac", "-profile:a", "aac_eld", "-eld_sbr", "1",
		"-ar", "16000", "-ac", "1",
		"-f", "null", "-")
	out, err := testCmd.CombinedOutput()
	log.Printf("[backchannel] -eld_sbr support: false (err=%v output=%s)", err, string(out))
	return false
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
	hasSBR := eldEncoder != "" && supportsELDSBR(eldEncoder)
	log.Printf("[backchannel] encoder=%q hasSBR=%v", eldEncoder, hasSBR)
	var started bool

	// Mode 1: Try libfdk_aac with RTP output (best: no LOAS parsing needed)
	// Test with SBR first if available, fall back to RTP without SBR
	rtpSBR := hasSBR && canEncodeELDViaRTP(eldEncoder, true)
	rtpNoSBR := !rtpSBR && eldEncoder != "" && canEncodeELDViaRTP(eldEncoder, false)
	if rtpSBR || rtpNoSBR {
		useSBR := rtpSBR
		outputConn, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err == nil {
			pipeline.udpConn = outputConn
			if err := pipeline.startRTPModeFDK(ctx, eldEncoder, sdpFileName,
				outputConn.LocalAddr().(*net.UDPAddr).Port, useSBR); err != nil {
				log.Printf("[backchannel] RTP-FDK mode failed: %v", err)
				outputConn.Close()
				pipeline.udpConn = nil
			} else {
				started = true
				log.Printf("[backchannel] === ACTIVE MODE: RTP-FDK (libfdk_aac → RTP, sbr=%v) ===", useSBR)
			}
		}
	}

	// Mode 2: Try libfdk_aac with LATM pipe (needs LOAS parsing)
	if !started && eldEncoder != "" {
		if err := pipeline.startPipeMode(ctx, eldEncoder, sdpFileName, hasSBR); err != nil {
			log.Printf("[backchannel] pipe mode failed: %v", err)
		} else {
			started = true
			log.Printf("[backchannel] === ACTIVE MODE: LOAS pipe (libfdk_aac → LATM → LOAS parse) ===")
		}
	}

	// Mode 3: Fall back to RTP mode with native AAC encoder
	if !started {
		outputConn, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			cancel()
			os.Remove(sdpFileName)
			return nil, nil, fmt.Errorf("backchannel: listen output: %w", err)
		}
		pipeline.udpConn = outputConn

		if err := pipeline.startRTPMode(ctx, sdpFileName, outputConn.LocalAddr().(*net.UDPAddr).Port); err != nil {
			pipeline.Close()
			return nil, nil, fmt.Errorf("backchannel: start ffmpeg: %w", err)
		}
		log.Printf("[backchannel] === ACTIVE MODE: native AAC (1024-sample frames) ===")
	}

	// Start goroutine to read AAC-ELD output and send to camera
	if pipeline.stdout != nil {
		go pipeline.readLOASAndSend(session, sendCounter)
	} else {
		go pipeline.readRTPAndSend(session, sendCounter)
	}

	// Create UDP connection to send Opus RTP to ffmpeg's input port
	ffmpegAddr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf("127.0.0.1:%d", inputPort))
	sendConn, err := net.DialUDP("udp", nil, ffmpegAddr)
	if err != nil {
		pipeline.Close()
		return nil, nil, fmt.Errorf("backchannel: dial ffmpeg: %w", err)
	}
	pipeline.sendConn = sendConn

	var seq uint16
	var handlerCount int
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
		handlerCount++

		if handlerCount <= 3 {
			log.Printf("[backchannel] handler sending Opus packet #%d: payloadLen=%d",
				handlerCount, len(packet.Payload))
		}

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

// startRTPModeFDK launches ffmpeg with libfdk_aac, outputting RFC 3640 RTP
// directly to a UDP port. This is the preferred mode because it avoids LOAS
// parsing entirely — ffmpeg handles the AAC → RTP framing natively.
func (p *backchannelPipeline) startRTPModeFDK(ctx context.Context, bin, sdpFile string, outputPort int, hasSBR bool) error {
	args := []string{
		"-hide_banner", "-loglevel", "info",
		"-protocol_whitelist", "file,rtp,udp",
		"-f", "sdp", "-i", sdpFile,
		"-c:a", "libfdk_aac", "-profile:a", "aac_eld",
	}
	if hasSBR {
		args = append(args, "-eld_sbr", "1")
	}
	args = append(args,
		"-ar", "16000", "-ac", "1", "-b:a", "32k",
		"-f", "rtp", fmt.Sprintf("rtp://127.0.0.1:%d", outputPort))

	cmd := exec.CommandContext(ctx, bin, args...)
	log.Printf("[backchannel] RTP-FDK cmd: %s", cmd.String())

	stderr, _ := cmd.StderrPipe()

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start: %w", err)
	}

	p.cmd = cmd

	go drainStderr("rtp-fdk", stderr)

	// Wait briefly to check if ffmpeg exits immediately (encoder error)
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	select {
	case err := <-done:
		return fmt.Errorf("ffmpeg exited immediately: %v", err)
	case <-time.After(500 * time.Millisecond):
		// Still running, good
		go func() {
			err := <-done
			if err != nil {
				log.Printf("[backchannel] RTP-FDK process exited: %v", err)
			} else {
				log.Printf("[backchannel] RTP-FDK process exited cleanly")
			}
		}()
	}

	return nil
}

// startPipeMode launches ffmpeg with libfdk_aac, outputting LATM to stdout.
// We parse LOAS frames from stdout and send raw AAC-ELD frames via SRTP.
func (p *backchannelPipeline) startPipeMode(ctx context.Context, bin, sdpFile string, hasSBR bool) error {
	args := []string{
		"-hide_banner", "-loglevel", "info",
		"-protocol_whitelist", "file,rtp,udp",
		"-f", "sdp", "-i", sdpFile,
		"-c:a", "libfdk_aac", "-profile:a", "aac_eld",
	}
	if hasSBR {
		args = append(args, "-eld_sbr", "1")
	}
	args = append(args, "-latm", "1",
		"-ar", "16000", "-ac", "1", "-b:a", "32k",
		"-f", "latm", "pipe:1")

	cmd := exec.CommandContext(ctx, bin, args...)
	log.Printf("[backchannel] pipe cmd: %s", cmd.String())

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("stdout pipe: %w", err)
	}

	stderr, _ := cmd.StderrPipe()

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start: %w", err)
	}

	p.cmd = cmd
	p.stdout = stdout

	go drainStderr("encoder", stderr)
	go func() {
		cmd.Wait()
		log.Printf("[backchannel] encoder process exited")
	}()

	return nil
}

// startRTPMode launches ffmpeg with native AAC encoder, outputting RTP to UDP.
func (p *backchannelPipeline) startRTPMode(ctx context.Context, sdpFile string, outputPort int) error {
	log.Printf("[backchannel] WARNING: using native AAC encoder (1024-sample frames)")

	cmd := exec.CommandContext(ctx, "ffmpeg",
		"-hide_banner", "-loglevel", "info",
		"-protocol_whitelist", "file,rtp,udp",
		"-f", "sdp", "-i", sdpFile,
		"-c:a", "aac", "-profile:a", "aac_eld",
		"-ar", "16000", "-ac", "1", "-b:a", "32k",
		"-f", "rtp", fmt.Sprintf("rtp://127.0.0.1:%d", outputPort))

	log.Printf("[backchannel] native cmd: %s", cmd.String())

	stderr, _ := cmd.StderrPipe()

	if err := cmd.Start(); err != nil {
		return err
	}

	p.cmd = cmd

	go drainStderr("native", stderr)
	go func() {
		cmd.Wait()
		log.Printf("[backchannel] native process exited")
	}()

	return nil
}

func drainStderr(name string, stderr interface{ Read([]byte) (int, error) }) {
	if stderr == nil {
		return
	}
	scanner := bufio.NewScanner(stderr)
	for scanner.Scan() {
		log.Printf("[backchannel] %s stderr: %s", name, scanner.Text())
	}
}

// readLOASAndSend reads LOAS frames from the encoder's stdout, extracts
// raw AAC-ELD frames, wraps them in RFC 3640, and sends via SRTP.
func (p *backchannelPipeline) readLOASAndSend(session *srtp.Session, sendCounter *int) {
	r := bufio.NewReaderSize(p.stdout, 4096)

	const timestampIncrement = 480
	var timestamp uint32
	var seq uint16
	var totalFrames int
	var firstFrame = true

	log.Printf("[backchannel] readLOASAndSend started")

	for {
		// Find LOAS sync word: 0x56, then byte with top 3 bits = 111
		b0, err := r.ReadByte()
		if err != nil {
			p.mu.Lock()
			closed := p.closed
			p.mu.Unlock()
			if !closed && err != io.EOF {
				log.Printf("[backchannel] LOAS read error: %v", err)
			}
			log.Printf("[backchannel] readLOASAndSend exiting, frames=%d", totalFrames)
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
			if totalFrames <= 3 {
				hexDump := fmt.Sprintf("%x", ameBuf)
				if len(hexDump) > 60 {
					hexDump = hexDump[:60] + "..."
				}
				log.Printf("[backchannel] LOAS: failed to extract AAC frame, ameLen=%d hex=%s",
					frameLen, hexDump)
			}
			continue
		}

		totalFrames++
		if totalFrames <= 5 {
			hexDump := fmt.Sprintf("%x", aacFrame)
			if len(hexDump) > 40 {
				hexDump = hexDump[:40] + "..."
			}
			log.Printf("[backchannel] LOAS frame #%d: aacLen=%d ameLen=%d aacHex=%s",
				totalFrames, len(aacFrame), frameLen, hexDump)
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
		} else if totalFrames <= 5 {
			log.Printf("[backchannel] WriteRTP error: %v", err)
		}
	}
}

// extractAACFromAME extracts the raw AAC frame from an AudioMuxElement.
func extractAACFromAME(data []byte, isFirst bool) []byte {
	if len(data) == 0 {
		return nil
	}

	if !isFirst {
		// useSameStreamMux should be 1 (bit 0 of first byte)
		if data[0]&0x80 == 0 {
			return extractFirstAME(data)
		}
		return extractPayloadFromBitOffset(data, 1)
	}

	return extractFirstAME(data)
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

func extractFirstAME(data []byte) []byte {
	return nil
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

// readRTPAndSend reads AAC-ELD RTP packets from ffmpeg's output UDP port
// and forwards to the camera via SRTP.
func (p *backchannelPipeline) readRTPAndSend(session *srtp.Session, sendCounter *int) {
	buf := make([]byte, 2048)

	const timestampIncrement = 480
	var timestamp uint32
	var seq uint16
	var totalRead int

	log.Printf("[backchannel] readRTPAndSend started, listening on %s", p.udpConn.LocalAddr())

	for {
		n, _, err := p.udpConn.ReadFrom(buf)
		if err != nil {
			p.mu.Lock()
			closed := p.closed
			p.mu.Unlock()
			if !closed {
				log.Printf("[backchannel] readRTPAndSend error: %v", err)
			}
			return
		}

		totalRead++

		packet := &rtp.Packet{}
		if err := packet.Unmarshal(buf[:n]); err != nil {
			continue
		}

		payload := packet.Payload
		if len(payload) < 4 {
			continue
		}

		// Unpack RFC 3640 AU headers
		auHeadersLenBits := int(payload[0])<<8 | int(payload[1])
		auHeadersLen := (auHeadersLenBits + 7) / 8
		numFrames := auHeadersLen / 2

		if numFrames == 0 || len(payload) < 2+auHeadersLen {
			continue
		}

		headers := payload[2 : 2+auHeadersLen]
		data := payload[2+auHeadersLen:]

		if totalRead <= 5 {
			hexDump := fmt.Sprintf("%x", payload)
			if len(hexDump) > 80 {
				hexDump = hexDump[:80] + "..."
			}
			log.Printf("[backchannel] ffmpeg RTP #%d: PT=%d SSRC=%d seq=%d ts=%d frames=%d dataLen=%d hex=%s",
				totalRead, packet.PayloadType, packet.SSRC, packet.SequenceNumber,
				packet.Timestamp, numFrames, len(data), hexDump)
		}

		for i := 0; i < numFrames && len(headers) >= 2; i++ {
			auSize := (int(headers[0])<<8 | int(headers[1])) >> 3
			headers = headers[2:]

			if auSize <= 0 || auSize > len(data) {
				break
			}

			frame := data[:auSize]
			data = data[auSize:]

			singlePayload := make([]byte, 4+len(frame))
			singlePayload[0] = 0x00
			singlePayload[1] = 0x10
			singlePayload[2] = byte((len(frame) << 3) >> 8)
			singlePayload[3] = byte((len(frame) << 3) & 0xFF)
			copy(singlePayload[4:], frame)

			singlePacket := &rtp.Packet{
				Header: rtp.Header{
					Version:        2,
					Marker:         true,
					SequenceNumber: seq,
					Timestamp:      timestamp,
				},
				Payload: singlePayload,
			}
			seq++
			timestamp += timestampIncrement

			if sent, err := session.WriteRTP(singlePacket); err == nil {
				*sendCounter += sent
			} else if totalRead <= 3 {
				log.Printf("[backchannel] send ERROR: %v", err)
			}
		}
	}
}

// Close shuts down ffmpeg process(es) and cleans up resources.
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
	if p.udpConn != nil {
		p.udpConn.Close()
	}
	if p.cmd != nil && p.cmd.Process != nil {
		p.cmd.Process.Kill()
	}
	if p.sdpFile != "" {
		os.Remove(p.sdpFile)
	}
	return nil
}
