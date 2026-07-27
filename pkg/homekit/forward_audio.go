package homekit

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/aac"
	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/pion/rtp"
)

// forwardAudioPipeline transcodes incoming ELD RTP (from camera SRTP) to Opus
// RTP (for WebRTC consumers) via ffmpeg.
type forwardAudioPipeline struct {
	mu       sync.Mutex
	closed   bool
	cancel   context.CancelFunc
	eldConn  *net.UDPConn   // sends ELD RTP to ffmpeg input
	opusConn net.PacketConn // receives Opus RTP from ffmpeg output
	sdpFile  string

	// crash recovery
	cmd        *exec.Cmd
	bin        string
	eldPort    int
	opusPort   int
	audioTrack *core.Receiver
	recvCounter *int
	configHex  string
}

// startForwardAudioPipeline sets up an ffmpeg process that decodes AAC-ELD RTP
// and re-encodes as Opus RTP. Returns the pipeline (caller feeds ELD packets
// via WriteELDPacket) and the output is written directly to audioTrack.
//
// configHex is the AudioSpecificConfig hex for the camera's AAC-ELD stream.
// If empty, a default for 16kHz mono ELD with LD-SBR is used.
func startForwardAudioPipeline(audioTrack *core.Receiver, recvCounter *int, configHex string) (*forwardAudioPipeline, error) {
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

	// Use provided ASC, or fall back to default for 16kHz mono ELD with LD-SBR
	if configHex == "" {
		configHex = "F8F0312C00BC00"
	}

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

	pipeline := &forwardAudioPipeline{
		sdpFile:     sdpFileName,
		opusConn:    opusListener,
		eldPort:     eldPort,
		opusPort:    opusPort,
		audioTrack:  audioTrack,
		recvCounter: recvCounter,
		bin:         ffmpegBin,
		configHex:   configHex,
	}

	if err := pipeline.startFFmpeg(); err != nil {
		opusListener.Close()
		os.Remove(sdpFileName)
		return nil, fmt.Errorf("forward: %w", err)
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
	go pipeline.readOpusAndForward()

	return pipeline, nil
}

// startFFmpeg launches the ffmpeg process for ELD→Opus transcoding.
func (p *forwardAudioPipeline) startFFmpeg() error {
	ctx, cancel := context.WithCancel(context.Background())

	cmd := exec.CommandContext(ctx, p.bin,
		"-hide_banner", "-loglevel", "error",
		"-c:a", "libfdk_aac",
		"-protocol_whitelist", "file,rtp,udp",
		"-f", "sdp", "-i", p.sdpFile,
		"-c:a", "libopus", "-ar", "48000", "-ac", "2", "-b:a", "64k",
		"-f", "rtp", fmt.Sprintf("rtp://127.0.0.1:%d", p.opusPort))

	stderr, _ := cmd.StderrPipe()

	if err := cmd.Start(); err != nil {
		cancel()
		return fmt.Errorf("start ffmpeg: %w", err)
	}

	p.mu.Lock()
	p.cmd = cmd
	p.cancel = cancel
	p.mu.Unlock()

	go drainStderr("forward-audio", stderr)

	go func() {
		err := cmd.Wait()
		p.mu.Lock()
		closed := p.closed
		p.mu.Unlock()
		if !closed && err != nil {
			log.Printf("[forward-audio] ffmpeg exited: %v — restarting", err)
			p.restart()
		}
	}()

	return nil
}

// restart relaunches the ffmpeg process after a crash.
func (p *forwardAudioPipeline) restart() {
	for i := 0; i < 3; i++ {
		p.mu.Lock()
		closed := p.closed
		p.mu.Unlock()
		if closed {
			return
		}

		time.Sleep(time.Second)
		log.Printf("[forward-audio] restart attempt %d/%d", i+1, 3)

		if err := p.startFFmpeg(); err != nil {
			log.Printf("[forward-audio] restart failed: %v", err)
			continue
		}
		log.Printf("[forward-audio] ffmpeg restarted successfully")
		return
	}
	log.Printf("[forward-audio] giving up after 3 restart attempts")
}

// WriteELDPacket forwards a decrypted ELD RTP packet from the camera to ffmpeg.
func (p *forwardAudioPipeline) WriteELDPacket(packet *rtp.Packet) {
	b, err := packet.Marshal()
	if err != nil {
		return
	}
	p.mu.Lock()
	if !p.closed && p.eldConn != nil {
		p.eldConn.Write(b)
	}
	p.mu.Unlock()
}

// readOpusAndForward reads Opus RTP packets from ffmpeg output and writes to track.
// Runs in a loop with crash recovery: if the opusConn read fails, it waits
// for ffmpeg to restart (handled by the cmd.Wait goroutine in startFFmpeg).
func (p *forwardAudioPipeline) readOpusAndForward() {
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

		p.audioTrack.WriteRTP(packet)
		*p.recvCounter += len(packet.Payload)
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