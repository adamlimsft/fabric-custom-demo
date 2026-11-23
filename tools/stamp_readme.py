"""Adds the contact line under the title of every README (and the AI guides). Idempotent. Called by build_kit.py."""
import glob, os

LINE = ("If you'd like a hand setting this up for a customer session or a training, feel free to email/message me "
        "(Adam Li liadam@microsoft.com). I'm happy to walk you through it or do a dry run with you.")
OLD = ("If you'd like a hand setting this up for a customer session or a class, feel free to email me "
       "(Adam Li liadam@microsoft.com). I'm happy to walk you through it or do a dry run with you.")


def targets(kit):
    files = glob.glob(os.path.join(kit, "**", "README.md"), recursive=True)
    files += glob.glob(os.path.join(kit, "AI", "*.md"))
    return sorted(set(files))


def stamp(path):
    with open(path, encoding="utf-8") as f:
        text = f.read()
    orig = text
    text = text.replace(OLD, LINE)
    lines = text.split("\n")
    if LINE not in text:
        i = 1 if lines and lines[0].startswith("# ") else 0
        lines[i:i] = ["", LINE] if i else [LINE, ""]
    # a divider straight after the intro line
    k = lines.index(LINE)
    j = k + 1
    while j < len(lines) and lines[j].strip() == "":
        j += 1
    if j >= len(lines) or lines[j].strip() != "---":
        lines[k + 1:j] = ["", "---", ""]
    text = "\n".join(lines)
    if text == orig:
        return False
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(text)
    return True


def main(kit):
    n = sum(stamp(p) for p in targets(kit))
    print(f"stamp_readme: {len(targets(kit))} files checked, {n} updated")


if __name__ == "__main__":
    main(os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")))
