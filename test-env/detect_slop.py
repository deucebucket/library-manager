#!/usr/bin/env python3
"""Local AI-slop detector for PR drafts and comments.

Uses a small HuggingFace RoBERTa model so it runs offline after the first
model download. Splits text into chunks at sentence boundaries and reports
paragraphs that score above a threshold as likely AI-generated.
"""
import argparse
import json
import re
import sys
from pathlib import Path


def split_into_chunks(text: str, max_chars: int = 1500):
    """Split text into chunks at sentence boundaries."""
    # Split on sentence endings followed by whitespace/capital letter/newline.
    sentences = re.split(r"(?<=[.!?])\s+(?=[A-Z#`\-]|\n)", text.strip())
    chunks = []
    current = ""
    for s in sentences:
        s = s.strip()
        if not s:
            continue
        if len(current) + len(s) + 1 > max_chars and current:
            chunks.append(current.strip())
            current = s
        else:
            current = f"{current} {s}".strip()
    if current:
        chunks.append(current.strip())
    return chunks


def load_detector(model_name: str = "roberta-base-openai-detector", device: int = -1):
    from transformers import pipeline

    print(f"Loading detector: {model_name} ...", file=sys.stderr)
    return pipeline("text-classification", model=model_name, device=device)


def scan_text(detector, text: str, threshold: float = 0.8):
    chunks = split_into_chunks(text)
    results = []
    for chunk in chunks:
        if not chunk.strip():
            continue
        pred = detector(chunk[:1500])[0]
        # roberta-base-openai-detector labels: Fake = AI, Real = human
        label = pred["label"]
        score = pred["score"]
        is_ai = label.lower() == "fake" and score >= threshold
        results.append(
            {
                "text": chunk,
                "label": label,
                "score": float(score),
                "flagged": bool(is_ai),
            }
        )
    return results


def main():
    parser = argparse.ArgumentParser(description="Detect AI-generated slop in text files.")
    parser.add_argument("files", nargs="+", help="Text files to scan")
    parser.add_argument("--model", default="roberta-base-openai-detector", help="HuggingFace model name")
    parser.add_argument("--threshold", type=float, default=0.8, help="AI probability threshold for flagging")
    parser.add_argument("--json", action="store_true", help="Output raw JSON")
    parser.add_argument("--device", type=int, default=-1, help="-1 for CPU, 0 for GPU")
    args = parser.parse_args()

    detector = load_detector(args.model, args.device)

    all_results = {}
    for path in args.files:
        text = Path(path).read_text(encoding="utf-8")
        results = scan_text(detector, text, args.threshold)
        all_results[path] = results

    if args.json:
        print(json.dumps(all_results, indent=2))
        return

    for path, results in all_results.items():
        print(f"\n=== {path} ===")
        flagged = [r for r in results if r["flagged"]]
        print(f"Chunks scanned: {len(results)} | Flagged: {len(flagged)}")
        for r in results:
            marker = "🚩 AI" if r["flagged"] else "   ok"
            snippet = r["text"].replace("\n", " ")[:120]
            print(f"{marker} ({r['label']} {r['score']:.2f}): {snippet}...")


if __name__ == "__main__":
    main()
