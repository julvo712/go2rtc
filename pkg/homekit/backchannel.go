package homekit

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"sync"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/srtp"
	"github.com/pion/rtp"
)

// backchannelPipeline manages the ffmpeg transcoding subprocess(es) that convert
// incoming Opus RTP packets (from WebRTC) to AAC-ELD RTP packets, which are
// then sent to the HomeKit camera via SRTP.
//
// Two modes:
//   - Two-process (libfdk_aac): encoder outputs LATM to pipe, muxer remuxes to RTP.
//     This bypasses libfdk_aac's inability to encode ELD with RAW transport
//     (which -f rtp requires via GLOBAL_HEADER).
//   - Single-process (native AAC): encoder outputs RTP directly.
//     Produces 1024-sample frames (vs 480 expected), split in readAndSend.
type backchannelPipeline struct {
	cmds     []*exec.Cmd
	cancel   context.CancelFunc
	sendConn *net.UDPConn   // sends Opus RTP to ffmpeg input
	udpConn  net.PacketConn // receives AAC-ELD RTP from ffmpeg output
	sdpFile  string         // temp SDP file path
	mu       sync.Mutex
	closed   bool
}

// findELDEncoder searches for an ffmpeg binary that can encode AAC-ELD
// using libfdk_aac with LATM output (which supports ELD, unlike -f rtp).
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
			"-c:a", "libfdk_aac", "-profile:a", "aac_eld", "-latm", "1",
			"-ar", "16000", "-ac", "1",
			"-f", "latm", os.DevNull)
		if err := testCmd.Run(); err == nil {
			return bin
		}
	}
	return ""
}

// startBackchannelPipeline spawns ffmpeg process(es) that:
// 1. Receive Opus RTP on a localhost UDP port
// 2. Transcode Opus → AAC-ELD (16kHz mono)
// 3. Output AAC-ELD RTP to another localhost UDP port
//
// It returns the pipeline and a handler function for incoming Opus RTP packets.
func startBackchannelPipeline(session *srtp.Session, sendCounter *int) (*backchannelPipeline, core.HandlerFunc, error) {
	// Open a UDP port for ffmpeg to listen on for Opus RTP input.
	// We bind it, get the port, then close — ffmpeg will bind it when it starts.
	inputAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		return nil, nil, fmt.Errorf("backchannel: resolve: %w", err)
	}
	inputTmp, err := net.ListenUDP("udp", inputAddr)
	if err != nil {
		return nil, nil, fmt.Errorf("backchannel: listen input: %w", err)
	}
	inputPort := inputTmp.LocalAddr().(*net.UDPAddr).Port
	inputTmp.Close() // release so ffmpeg can bind it

	// Open a UDP port for receiving AAC-ELD RTP output from ffmpeg
	outputConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		return nil, nil, fmt.Errorf("backchannel: listen output: %w", err)
	}
	outputPort := outputConn.LocalAddr().(*net.UDPAddr).Port

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
		outputConn.Close()
		return nil, nil, fmt.Errorf("backchannel: create sdp: %w", err)
	}
	sdpFileName := sdpFile.Name()
	if _, err := sdpFile.WriteString(sdp); err != nil {
		outputConn.Close()
		sdpFile.Close()
		os.Remove(sdpFileName)
		return nil, nil, fmt.Errorf("backchannel: write sdp: %w", err)
	}
	sdpFile.Close()

	ctx, cancel := context.WithCancel(context.Background())

	pipeline := &backchannelPipeline{
		udpConn: outputConn,
		sdpFile: sdpFileName,
		cancel:  cancel,
	}

	// Try two-process mode with libfdk_aac (proper 480-sample AAC-ELD frames)
	eldEncoder := findELDEncoder()
	if eldEncoder != "" {
		err = pipeline.startTwoProcess(ctx, eldEncoder, sdpFileName, outputPort)
		if err != nil {
			log.Printf("[backchannel] two-process start failed: %v, falling back to native", err)
			eldEncoder = "" // fall through to single-process
		}
	}

	// Fall back to single-process with native AAC encoder
	if eldEncoder == "" {
		err = pipeline.startSingleProcess(ctx, sdpFileName, outputPort)
		if err != nil {
			pipeline.Close()
			return nil, nil, fmt.Errorf("backchannel: start ffmpeg: %w", err)
		}
	}

	// Start goroutine to read AAC-ELD RTP from ffmpeg output and send to camera
	go pipeline.readAndSend(session, sendCounter)

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
			log.Printf("[backchannel] handler sending Opus packet #%d: payloadLen=%d ts=%d",
				handlerCount, len(packet.Payload), packet.Timestamp)
		}

		b, err := clone.Marshal()
		if err != nil {
			return
		}
		sendConn.Write(b)
	}

	// Clean up send socket when pipeline context is cancelled
	go func() {
		<-ctx.Done()
		sendConn.Close()
	}()

	return pipeline, handler, nil
}

// startTwoProcess launches a two-process pipeline:
//
//	encoder (libfdk_aac → LATM stdout) | muxer (LATM stdin → RTP UDP)
//
// This works around libfdk_aac's inability to encode AAC-ELD when the output
// format sets GLOBAL_HEADER (which selects RAW transport — unsupported for ELD).
// LATM output uses LOAS transport which supports ELD.
func (p *backchannelPipeline) startTwoProcess(ctx context.Context, encoderBin, sdpFile string, outputPort int) error {
	log.Printf("[backchannel] using two-process mode: %s (libfdk_aac → LATM | LATM → RTP)", encoderBin)

	// Process 1: Opus RTP → AAC-ELD LATM (stdout)
	encoder := exec.CommandContext(ctx, encoderBin,
		"-hide_banner", "-loglevel", "error",
		"-protocol_whitelist", "file,rtp,udp",
		"-f", "sdp", "-i", sdpFile,
		"-c:a", "libfdk_aac", "-profile:a", "aac_eld", "-latm", "1",
		"-ar", "16000", "-ac", "1", "-b:a", "32k",
		"-f", "latm", "pipe:1")

	// Process 2: LATM (stdin) → RTP UDP
	// Use the same binary as the encoder — system ffmpeg may lack LATM demuxer
	muxer := exec.CommandContext(ctx, encoderBin,
		"-hide_banner", "-loglevel", "error",
		"-f", "latm", "-i", "pipe:0",
		"-c:a", "copy",
		"-f", "rtp", fmt.Sprintf("rtp://127.0.0.1:%d", outputPort))

	// Connect encoder stdout → muxer stdin via OS pipe
	pr, pw, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("os.Pipe: %w", err)
	}
	encoder.Stdout = pw
	muxer.Stdin = pr

	// Capture stderr from both processes
	encoderStderr, _ := encoder.StderrPipe()
	muxerStderr, _ := muxer.StderrPipe()

	if err := encoder.Start(); err != nil {
		pr.Close()
		pw.Close()
		return fmt.Errorf("start encoder: %w", err)
	}
	if err := muxer.Start(); err != nil {
		encoder.Process.Kill()
		pr.Close()
		pw.Close()
		return fmt.Errorf("start muxer: %w", err)
	}

	// Close parent's pipe ends — children have inherited their own fds
	pw.Close()
	pr.Close()

	p.cmds = []*exec.Cmd{encoder, muxer}

	// Drain stderr from both processes
	go drainStderr("encoder", encoderStderr)
	go drainStderr("muxer", muxerStderr)

	// Log when processes exit
	go func() {
		encoder.Wait()
		log.Printf("[backchannel] encoder process exited")
	}()
	go func() {
		muxer.Wait()
		log.Printf("[backchannel] muxer process exited")
	}()

	return nil
}

// startSingleProcess launches a single ffmpeg with native AAC encoder.
// Produces 1024-sample frames (split in readAndSend).
func (p *backchannelPipeline) startSingleProcess(ctx context.Context, sdpFile string, outputPort int) error {
	log.Printf("[backchannel] WARNING: using single-process mode with native AAC encoder (1024-sample frames)")

	cmd := exec.CommandContext(ctx, "ffmpeg",
		"-hide_banner", "-loglevel", "error",
		"-protocol_whitelist", "file,rtp,udp",
		"-f", "sdp", "-i", sdpFile,
		"-c:a", "aac", "-profile:a", "aac_eld",
		"-ar", "16000", "-ac", "1", "-b:a", "32k",
		"-f", "rtp", fmt.Sprintf("rtp://127.0.0.1:%d", outputPort))

	stderr, _ := cmd.StderrPipe()

	if err := cmd.Start(); err != nil {
		return err
	}

	p.cmds = []*exec.Cmd{cmd}

	go drainStderr("ffmpeg", stderr)
	go func() {
		cmd.Wait()
		log.Printf("[backchannel] ffmpeg process exited")
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

// readAndSend reads AAC-ELD RTP packets from ffmpeg's output UDP port
// and forwards them to the HomeKit camera via SRTP.
//
// Both pipeline modes output RFC 3640 RTP packets. The native encoder
// packs multiple 1024-sample frames per packet; the libfdk_aac/LATM
// pipeline produces single 480-sample frames per packet.
// We unpack AU headers and send each frame individually with 480-sample
// timestamp increments (30ms at 16kHz), as HomeKit cameras expect.
func (p *backchannelPipeline) readAndSend(session *srtp.Session, sendCounter *int) {
	buf := make([]byte, 2048)

	const timestampIncrement = 480
	var timestamp uint32
	var seq uint16
	var totalRead int

	log.Printf("[backchannel] readAndSend started, listening on %s", p.udpConn.LocalAddr())

	for {
		n, _, err := p.udpConn.ReadFrom(buf)
		if err != nil {
			p.mu.Lock()
			closed := p.closed
			p.mu.Unlock()
			if !closed {
				log.Printf("[backchannel] readAndSend error: %v", err)
			}
			log.Printf("[backchannel] readAndSend exiting, totalRead=%d", totalRead)
			return
		}

		totalRead++
		if totalRead <= 5 {
			log.Printf("[backchannel] readAndSend packet #%d: %d bytes", totalRead, n)
		}

		// Parse the RTP packet from ffmpeg
		packet := &rtp.Packet{}
		if err := packet.Unmarshal(buf[:n]); err != nil {
			log.Printf("[backchannel] unmarshal error: %v", err)
			continue
		}

		// Unpack RFC 3640 AU headers and send each AAC frame individually
		payload := packet.Payload
		if len(payload) < 4 {
			continue
		}

		auHeadersLenBits := int(payload[0])<<8 | int(payload[1])
		auHeadersLen := (auHeadersLenBits + 7) / 8
		numFrames := auHeadersLen / 2

		if numFrames == 0 || len(payload) < 2+auHeadersLen {
			continue
		}

		headers := payload[2 : 2+auHeadersLen]
		data := payload[2+auHeadersLen:]

		if totalRead <= 5 {
			log.Printf("[backchannel] PT=%d frames=%d dataLen=%d ts=%d",
				packet.PayloadType, numFrames, len(data), packet.Timestamp)
		}

		for i := 0; i < numFrames && len(headers) >= 2; i++ {
			auSize := (int(headers[0])<<8 | int(headers[1])) >> 3
			headers = headers[2:]

			if auSize <= 0 || auSize > len(data) {
				break
			}

			frame := data[:auSize]
			data = data[auSize:]

			// Wrap single frame in RFC 3640 format
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
			} else if totalRead <= 5 {
				log.Printf("[backchannel] WriteRTP error: %v", err)
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
	if p.udpConn != nil {
		p.udpConn.Close()
	}
	for _, cmd := range p.cmds {
		if cmd.Process != nil {
			cmd.Process.Kill()
		}
	}
	if p.sdpFile != "" {
		os.Remove(p.sdpFile)
	}
	return nil
}
