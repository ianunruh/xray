// Lazy AudioContext — some browsers refuse to construct one before a
// user gesture, and we don't want a module-load throw to take down the
// app. First call from a button-click code path (e.g. order submit)
// reliably succeeds; subsequent calls reuse the same context.
let ctx: AudioContext | null = null;

function getCtx(): AudioContext | null {
  if (ctx) return ctx;
  try {
    const AC =
      window.AudioContext ??
      (window as unknown as { webkitAudioContext?: typeof AudioContext })
        .webkitAudioContext;
    if (!AC) return null;
    ctx = new AC();
    return ctx;
  } catch {
    return null;
  }
}

// playFillDing plays a short two-partial bell tone — fundamental at
// A5 with an A6 overtone, both exponentially decaying — used as the
// "order filled" auditory cue. Silently no-ops if Web Audio is
// unavailable or the context can't be resumed.
export function playFillDing() {
  const c = getCtx();
  if (!c) return;
  // Browsers suspend the context after long inactivity; resume is
  // a no-op when already running.
  if (c.state === "suspended") {
    c.resume().catch(() => {});
  }

  const now = c.currentTime;
  const partials = [
    { freq: 880, gain: 0.25, decay: 0.6 },
    { freq: 1760, gain: 0.1, decay: 0.4 },
  ];
  for (const p of partials) {
    const osc = c.createOscillator();
    osc.type = "sine";
    osc.frequency.value = p.freq;
    const gain = c.createGain();
    gain.gain.setValueAtTime(p.gain, now);
    gain.gain.exponentialRampToValueAtTime(0.0001, now + p.decay);
    osc.connect(gain).connect(c.destination);
    osc.start(now);
    osc.stop(now + p.decay);
  }
}
