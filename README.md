# htfs - Hash-Tag File System

> [!warning]
> htfs is in early stages of development, and not suitable for daily use

Prefixed tag based, hierarchic file system.

### Example

```bash
tree --inodes mountpoint
```

```
[      1]  mountpoint/
├── [      3]  #Documents
│   ├── [      6]  downloaded-document.pdf
│   ├── [      5]  #Downloads
│   │   ├── [      6]  downloaded-document.pdf
│   │   └── [      7]  Images
│   │       └── [      9]  img.png
│   └── [      7]  Images
│       └── [      9]  img.png
├── [      2]  #Downloads
│   ├── [      4]  #Documents
│   │   ├── [      6]  downloaded-document.pdf
│   │   └── [      7]  Images
│   │       └── [      9]  img.png
│   ├── [      6]  downloaded-document.pdf
│   └── [      7]  Images
│       └── [      9]  img.png
└── [      8]  Images

10 directories, 8 files
```

###### TODO

- handle hard links
- calculate directory size
- handle superset duplicates (e.g. $f \in A \cup B$, $f' \in A$ where $f$ and $f'$ has the same name)
