package homekit

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"sync"

	"github.com/AlexxIT/go2rtc/pkg/aac"
	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/pion/rtp"
)

// forwardAudioPipeline transcodes incoming ELD RTP (from camera SRTP) to Opus
// RTP (for WebRTC consumers) via ffmpeg.
type forwardAudioPipeline struct {
	cmd      *exec.Cmd
	cancel   context.CancelFunc
	eldConn  *net.UDPConn   // sends ELD RTP to ffmpeg input
	opusConn net.PacketConn // receives Opus RTP from ffmpeg output
	sdpFile  string
	mu       sync.Mutex
	closed   bool
}

// startForwardAudioPipeline sets up an ffmpeg process that decodes AAC-ELD RTP
// and re-encodes as Opus RTP. Returns the pipeline (caller feeds ELD packets
// via WriteELDPacket) and the output is written directly to audioTrack.
func startForwardAudioPipeline(audioTrack *core.Receiver, recvCounter *int) (*forwardAudioPipeline, error) {
	// Find free ports for ffmpeg input (ELD) and output (Opus)
	eldListener, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("forward: listen eld: %w", err)
	}
	eldPort := eldListener.LocalAddr().(*net.UDPAddr).Port

	opusListener, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		eldListener.Close()
		return nil, fmt.Errorf("forward: listen opus: %w", err)
	}
	opusPort := opusListener.LocalAddr().(*net.UDPAddr).Port

	// AudioSpecificConfig for AAC-ELD 16kHz mono with LD-SBR, 480-sample frames.
	configHex := "F8F0312C00BC00"

	sdp := fmt.Sprintf(
		"v=0\r\n"+
			"o=- 0 0 IN IP4 127.0.0.1\r\n"+
			"s=forward\r\n"+
			"c=IN IP4 127.0.0.1\r\n"+
			"t=0 0\r\n"+
			"m=audio %d RTP/AVP 110\r\n"+
			"a=rtpmap:110 MPEG4-GENERIC/16000/1\r\n"+
			"a=fmtp:110 %s%s\r\n",
		eldPort, aac.FMTP, configHex,
	)

	sdpFile, err := os.CreateTemp("", "go2rtc-forward-*.sdp")
	if err != nil {
		eldListener.Close()
		opusListener.Close()
		return nil, fmt.Errorf("forward: create sdp: %w", err)
	}
	sdpFileName := sdpFile.Name()
	sdpFile.WriteString(sdp)
	sdpFile.Close()

	// Close the ELD listener so ffmpeg can bind to the same port
	eldListener.Close()

	// Use ffmpeg with libfdk_aac for decoding — the native AAC decoder
	// doesn't support Low Delay SBR which HomeKit cameras use.
	ffmpegBin := findELDEncoder()
	if ffmpegBin == "" {
		ffmpegBin = "ffmpeg"
	}

	ctx, cancel := context.WithCancel(context.Background())

	cmd := exec.CommandContext(ctx, ffmpegBin,
		"-hide_banner", "-loglevel", "error",
		"-c:a", "libfdk_aac",
		"-protocol_whitelist", "file,rtp,udp",
		"-f", "sdp", "-i", sdpFileName,
		"-c:a", "libopus", "-ar", "48000", "-ac", "2", "-b:a", "64k",
		"-f", "rtp", fmt.Sprintf("rtp://127.0.0.1:%d", opusPort))

	stderr, _ := cmd.StderrPipe()

	if err := cmd.Start(); err != nil {
		cancel()
		opusListener.Close()
		os.Remove(sdpFileName)
		return nil, fmt.Errorf("forward: start ffmpeg: %w", err)
	}

	go drainStderr("forward-audio", stderr)

	pipeline := &forwardAudioPipeline{
		cmd:      cmd,
		cancel:   cancel,
		opusConn: opusListener,
		sdpFile:  sdpFileName,
	}

	// Create UDP connection to send ELD RTP to ffmpeg's input port
	eldAddr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf("127.0.0.1:%d", eldPort))
	eldSendConn, err := net.DialUDP("udp", nil, eldAddr)
	if err != nil {
		pipeline.Close()
		return nil, fmt.Errorf("forward: dial eld: %w", err)
	}
	pipeline.eldConn = eldSendConn

	// Read Opus RTP output from ffmpeg and write to audio track
	go pipeline.readOpusAndForward(audioTrack, recvCounter)

	go func() {
		cmd.Wait()
		log.Printf("[forward-audio] ffmpeg exited")
	}()

	return pipeline, nil
}

// WriteELDPacket forwards a decrypted ELD RTP packet from the camera to ffmpeg.
func (p *forwardAudioPipeline) WriteELDPacket(packet *rtp.Packet) {
	b, err := packet.Marshal()
	if err != nil {
		return
	}
	p.eldConn.Write(b)
}

// readOpusAndForward reads Opus RTP packets from ffmpeg output and writes to track.
func (p *forwardAudioPipeline) readOpusAndForward(track *core.Receiver, recvCounter *int) {
	buf := make([]byte, 2048)

	for {
		n, _, err := p.opusConn.ReadFrom(buf)
		if err != nil {
			p.mu.Lock()
			closed := p.closed
			p.mu.Unlock()
			if !closed {
				log.Printf("[forward-audio] read error: %v", err)
			}
			return
		}

		packet := &rtp.Packet{}
		if err := packet.Unmarshal(buf[:n]); err != nil {
			continue
		}

		track.WriteRTP(packet)
		*recvCounter += len(packet.Payload)
	}
}

func (p *forwardAudioPipeline) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}
	p.closed = true

	if p.cancel != nil {
		p.cancel()
	}
	if p.eldConn != nil {
		p.eldConn.Close()
	}
	if p.opusConn != nil {
		p.opusConn.Close()
	}
	if p.cmd != nil && p.cmd.Process != nil {
		p.cmd.Process.Kill()
	}
	if p.sdpFile != "" {
		os.Remove(p.sdpFile)
	}
	return nil
}
