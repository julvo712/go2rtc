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
// incoming Opus RTP packets (from WebRTC) to AAC-ELD and sends them to the
// HomeKit camera via SRTP.
//
// Uses libfdk_aac with -latm 1 (LOAS transport, required for AAC-ELD) and
// -f latm muxer (passes through LOAS frames from the encoder). The Go side
// parses LOAS frames, extracts raw AAC access units, wraps them in RFC 3640,
// and sends via SRTP.
type backchannelPipeline struct {
	mu       sync.Mutex
	closed   bool
	cancel   context.CancelFunc
	sendConn *net.UDPConn  // sends Opus RTP to ffmpeg input
	stdout   io.ReadCloser // encoder stdout (LOAS stream)
	sdpFile  string

	// crash recovery
	cmd       *exec.Cmd
	bin       string
	encoderArgs []string
	session   *srtp.Session
	sendCounter *int
	tsIncrement uint32 // samples per frame, derived from codec clock rate
}

// cachedELDEncoder is the result of findELDEncoder(), cached after first probe
// to avoid spawning 3+ ffmpeg test processes on every backchannel start.
var (
	cachedELDEncoder     string
	cachedELDEncoderOnce sync.Once
)

// cachedELDOptions caches probe results for -frame_length and -eld_sbr
var (
	cachedFrameLength bool
	cachedEldSbr      bool
	cachedOptionsOnce sync.Once
)

func findELDEncoder() string {
	cachedELDEncoderOnce.Do(func() {
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
				cachedELDEncoder = bin
				return
			}
		}
	})
	return cachedELDEncoder
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

// probeELDOptions probes -frame_length 480 and -eld_sbr 1 support once and caches.
func probeELDOptions(bin string) (frameLength, eldSbr bool) {
	cachedOptionsOnce.Do(func() {
		cachedFrameLength = testELDOption(bin, "-frame_length", "480")
		cachedEldSbr = testELDOption(bin, "-eld_sbr", "1")
	})
	return cachedFrameLength, cachedEldSbr
}

func startBackchannelPipeline(session *srtp.Session, sendCounter *int, clockRate uint32) (*backchannelPipeline, core.HandlerFunc, error) {
	// Open a UDP port for ffmpeg to listen on for Opus RTP input.
	// Keep the listener open (don't close+rebind) to avoid TOCTOU race.
	inputAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		return nil, nil, fmt.Errorf("backchannel: resolve: %w", err)
	}
	inputConn, err := net.ListenUDP("udp", inputAddr)
	if err != nil {
		return nil, nil, fmt.Errorf("backchannel: listen input: %w", err)
	}
	inputPort := inputConn.LocalAddr().(*net.UDPAddr).Port
	// Don't close — ffmpeg needs to bind to this port, and we keep our
	// sendConn pointing at it. The listener stays open for the pipeline lifetime.
	// Actually we need to close the listener so ffmpeg can bind. But to avoid
	// TOCTOU, we close it right before starting ffmpeg, minimizing the window.
	inputConn.Close()

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

	eldEncoder := findELDEncoder()
	if eldEncoder == "" {
		cancel()
		os.Remove(sdpFileName)
		return nil, nil, fmt.Errorf("backchannel: no ffmpeg with libfdk_aac found")
	}

	// Derive timestamp increment from clock rate.
	// AAC-ELD uses 480-sample frames at 16kHz = 30ms.
	// At 24kHz the increment would be 720.
	tsIncrement := uint32(480)
	if clockRate > 0 {
		// 480 samples per frame, scaled by clock rate relative to 16kHz base
		tsIncrement = uint32(480 * clockRate / 16000)
	}

	pipeline := &backchannelPipeline{
		sdpFile:     sdpFileName,
		cancel:      cancel,
		session:     session,
		sendCounter: sendCounter,
		tsIncrement: tsIncrement,
	}

	if err := pipeline.startEncoder(ctx, eldEncoder, sdpFileName); err != nil {
		cancel()
		os.Remove(sdpFileName)
		return nil, nil, fmt.Errorf("backchannel: %w", err)
	}

	go pipeline.readLOASAndSend()

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
		pipeline.mu.Lock()
		if !pipeline.closed && pipeline.sendConn != nil {
			pipeline.sendConn.Write(b)
		}
		pipeline.mu.Unlock()
	}

	go func() {
		<-ctx.Done()
		pipeline.mu.Lock()
		if pipeline.sendConn != nil {
			pipeline.sendConn.Close()
			pipeline.sendConn = nil
		}
		pipeline.mu.Unlock()
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
		// Increase read timeout: WebRTC mic audio may start with a delay
		// while the user grants mic permission. Default timeout is too short.
		"-rw_timeout", "10000000", // 10s in microseconds
		"-f", "sdp", "-i", sdpFile,
		"-c:a", "libfdk_aac", "-profile:a", "aac_eld",
		"-latm", "1",
	}
	frameLength, eldSbr := probeELDOptions(bin)
	if frameLength {
		args = append(args, "-frame_length", "480")
	}
	if eldSbr {
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

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start: %w", err)
	}

	p.cmd = cmd
	p.bin = bin
	p.encoderArgs = args
	p.stdout = stdout

	// Capture stderr for debugging — was missing entirely before
	go drainStderr("backchannel", stderr)

	return nil
}

func drainStderr(name string, stderr io.Reader) {
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
// Runs in a loop with crash recovery: if ffmpeg dies, it restarts the encoder.
func (p *backchannelPipeline) readLOASAndSend() {
	for {
		p.mu.Lock()
		closed := p.closed
		p.mu.Unlock()
		if closed {
			return
		}

		r := bufio.NewReaderSize(p.stdout, 4096)

		var timestamp uint32
		var seq uint16
		var firstFrame = true

		for {
			// Find LOAS sync word: 0x56, then byte with top 3 bits = 111
			b0, err := r.ReadByte()
			if err != nil {
				log.Printf("[backchannel] stdout read error: %v — restarting encoder", err)
				break
			}
			if b0 != 0x56 {
				continue
			}

			b1, err := r.ReadByte()
			if err != nil {
				log.Printf("[backchannel] stdout read error: %v — restarting encoder", err)
				break
			}
			if b1&0xE0 != 0xE0 {
				r.UnreadByte()
				continue
			}

			b2, err := r.ReadByte()
			if err != nil {
				log.Printf("[backchannel] stdout read error: %v — restarting encoder", err)
				break
			}

			// Frame length from 13 bits: 5 bits from b1 + 8 bits from b2
			frameLen := int(b1&0x1F)<<8 | int(b2)
			if frameLen == 0 || frameLen > 8192 {
				continue
			}

			// Read AudioMuxElement
			ameBuf := make([]byte, frameLen)
			if _, err := io.ReadFull(r, ameBuf); err != nil {
				log.Printf("[backchannel] frame read error: %v — restarting encoder", err)
				break
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
			timestamp += p.tsIncrement

			if sent, err := p.session.WriteRTP(packet); err == nil {
				*p.sendCounter += sent
			}
		}

		// ffmpeg crashed or stdout EOF — attempt restart
		p.mu.Lock()
		closed = p.closed
		p.mu.Unlock()
		if closed {
			return
		}

		log.Printf("[backchannel] encoder crashed, restarting in 1s...")
		time.Sleep(time.Second)

		// Recreate context for the new ffmpeg process
		ctx, cancel := context.WithCancel(context.Background())
		p.mu.Lock()
		p.cancel = cancel
		p.mu.Unlock()

		// Reuse existing SDP file (still on disk)
		if err := p.startEncoder(ctx, p.bin, p.sdpFile); err != nil {
			log.Printf("[backchannel] encoder restart failed: %v — giving up", err)
			cancel()
			return
		}
		log.Printf("[backchannel] encoder restarted successfully")
	}
}

// extractAACFromAME extracts the raw AAC frame from an AudioMuxElement.
// The first AME (useSameStreamMux=0) contains StreamMuxConfig and is skipped.
// Subsequent AMEs (useSameStreamMux=1) contain PayloadLengthInfo + payload.
func extractAACFromAME(data []byte, isFirst bool) []byte {
	if len(data) == 0 {
		return nil
	}

	// useSameStreamMux is the top bit. If it's 0, this frame contains
	// StreamMuxConfig — skip it (but only if we haven't seen any config yet).
	// After the first frame, a 0 bit means a new StreamMuxConfig (config refresh).
	if data[0]&0x80 == 0 {
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