import re

pattern = re.compile(r'^\s*(?:left|right)-([0-9]+\.[0-9]+)\s*$', re.IGNORECASE)

tests = [
    "left", " right ", "left-3,0", "RIGHT-12,34",
    "left-3", "left-3.0", "left-3,", "some left", "left- 3,0"
]

for s in tests:
    m = pattern.match(s)
    print(s, "=>", bool(m), "num=", m.group(1) if m else None)

