"""Pads markdown tables so the columns line up in a plain-text editor as well as when rendered."""
import re, sys

def split(row):
    cells = re.split(r'(?<!\\)\|', row.strip()[1:-1])
    return [c.strip() for c in cells]

def fmt(block):
    rows = [split(r) for r in block]
    n = max(len(r) for r in rows)
    rows = [r + [''] * (n - len(r)) for r in rows]
    w = [max(3, max(len(r[i]) for j, r in enumerate(rows) if j != 1)) for i in range(n)]
    out = []
    for j, r in enumerate(rows):
        cells = ['-' * w[i] for i in range(n)] if j == 1 else [r[i].ljust(w[i]) for i in range(n)]
        out.append('| ' + ' | '.join(cells) + ' |')
    return out

def align(path):
    lines = open(path, encoding='utf-8').read().split('\n')
    out, block, fence = [], [], False
    for l in lines + ['']:
        if l.startswith('```'):
            fence = not fence
        if not fence and l.startswith('|'):
            block.append(l); continue
        if block:
            out += fmt(block); block = []
        out.append(l)
    open(path, 'w', encoding='utf-8', newline='\n').write('\n'.join(out[:-1]))

if __name__ == '__main__':
    for p in sys.argv[1:]:
        align(p)
