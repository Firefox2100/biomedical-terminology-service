"""
Split a large `*.doc.dump` file (line-separated JSON documents, as produced by any
vocabulary/annotation module's `--offline` mode - see docs/source/build-database.rst)
into a series of smaller JSONL chunk files of a target maximum size each.

Two modes are available:

* Default (copy) mode is a pure line-based streaming split: the input is read one line
  at a time and written straight through to the current chunk file, so memory usage
  stays constant (a single line buffer) regardless of input size. The input is left
  untouched, so peak disk usage is roughly input size + output size (~2x the input).

* ``--in-place`` mode instead peels a line-aligned block off the *tail* of the input
  file, writes it out as a chunk, ``fsync``s it, and only then truncates the input file
  to drop that block. Shrinking a file from the end (``ftruncate``) is a cheap metadata
  operation - no data is rewritten - unlike removing from the front, which would require
  rewriting the entire remaining file. Because the chunk is fully written and flushed to
  disk before the source is truncated, peak disk usage is only input size + one chunk
  (not 2x the input), and an interruption mid-run can only ever leave a partial chunk
  file to discard - the source is never truncated until its data is safely elsewhere.
  This is nonetheless a destructive, irreversible operation on the input file: only use
  it once you're confident the chunks already written are good (e.g. after spot-checking
  or uploading them), and never on a file you don't have a way to regenerate/redownload.

Order of documents across/within chunks is not meaningful either way (each line is an
independent JSON document), so chunks can be uploaded, restored, or reordered freely.

Usage:

    python split_doc_dump.py path/to/some-prefix.doc.dump
    python split_doc_dump.py path/to/some-prefix.doc.dump --chunk-size-gb 50
    python split_doc_dump.py path/to/some-prefix.doc.dump --output-dir /some/dir
    python split_doc_dump.py path/to/some-prefix.doc.dump --in-place   # shrinks the input as it goes

Produces `some-prefix.doc.dump.part000`, `some-prefix.doc.dump.part001.`, ...
next to the input file (or under --output-dir if given), each at most --chunk-size-gb
in size (a chunk may be a few KB under/over the limit, since a line is never split
mid-record). In ``--in-place`` mode, part numbers are assigned in the order chunks are
cut (tail of the file first), which is unrelated to their position in the original file.

Each part is plain JSONL and can be imported the same way as the original dump, e.g.:

    mongoimport --db bioterms --collection <collection> --file some-prefix.doc.dump.part000
"""

import argparse
import os
import sys


def split_doc_dump(input_path: str,
                    output_dir: str | None = None,
                    chunk_size_bytes: int = 50 * 1024 ** 3,
                    ) -> list[str]:
    """
    Split a line-separated JSON document dump file into size-bounded JSONL chunks,
    leaving the input file untouched. Peak disk usage is ~input size + output size.

    :param input_path: Path to the source `.doc.dump` (or any other newline-delimited
        JSON) file.
    :param output_dir: Directory to write chunk files into. Defaults to the input
        file's own directory.
    :param chunk_size_bytes: Maximum size, in bytes, for each chunk file. A chunk is
        closed and a new one started as soon as this is reached, on a line boundary.
    :return: The list of chunk file paths written, in order.
    """
    if output_dir is None:
        output_dir = os.path.dirname(os.path.abspath(input_path))
    os.makedirs(output_dir, exist_ok=True)

    base_name = os.path.basename(input_path)
    chunk_paths = []

    chunk_index = 0
    bytes_in_chunk = 0
    out_file = None

    def open_next_chunk():
        nonlocal chunk_index, bytes_in_chunk, out_file
        if out_file is not None:
            out_file.close()
        chunk_path = os.path.join(output_dir, f'{base_name}.part{chunk_index:03d}')
        chunk_paths.append(chunk_path)
        out_file = open(chunk_path, 'w', encoding='utf-8')
        chunk_index += 1
        bytes_in_chunk = 0

    try:
        with open(input_path, 'r', encoding='utf-8') as in_file:
            open_next_chunk()

            for line in in_file:
                if not line.strip():
                    continue

                if bytes_in_chunk > 0 and bytes_in_chunk + len(line.encode('utf-8')) > chunk_size_bytes:
                    open_next_chunk()

                out_file.write(line if line.endswith('\n') else line + '\n')
                bytes_in_chunk += len(line.encode('utf-8'))
    finally:
        if out_file is not None:
            out_file.close()

    return chunk_paths


def _find_forward_line_boundary(input_path: str, start: int, scan_buffer: int = 4 * 1024 ** 2) -> int:
    """
    Starting at byte offset `start`, scan forward for the next newline and return the
    offset of the byte right after it (i.e. the start of the next whole line). Returns
    0 if no newline is found before EOF (the whole file is a single unsplit line).
    """
    if start <= 0:
        return 0

    with open(input_path, 'rb') as f:
        f.seek(start)
        offset = start
        while True:
            block = f.read(scan_buffer)
            if not block:
                return 0
            idx = block.find(b'\n')
            if idx != -1:
                return offset + idx + 1
            offset += len(block)


def _copy_range(input_path: str, output_path: str, start: int, end: int, buffer_size: int = 64 * 1024 ** 2) -> None:
    """Stream-copy bytes [start, end) from input_path into a new output_path, fsync'd before returning."""
    with open(input_path, 'rb') as fin, open(output_path, 'wb') as fout:
        fin.seek(start)
        remaining = end - start
        while remaining > 0:
            block = fin.read(min(buffer_size, remaining))
            if not block:
                break
            fout.write(block)
            remaining -= len(block)
        fout.flush()
        os.fsync(fout.fileno())


def split_doc_dump_in_place(input_path: str,
                             output_dir: str | None = None,
                             chunk_size_bytes: int = 50 * 1024 ** 3,
                             ) -> list[str]:
    """
    Split a line-separated JSON document dump file into size-bounded JSONL chunks,
    shrinking the input file as each chunk is cut from its tail. DESTRUCTIVE: the input
    file is truncated in place and cannot be recovered once this returns/is interrupted
    partway - each chunk is only cut after it has been fully written and fsync'd to
    disk, so a crash mid-run leaves the input intact minus whatever chunks were already
    safely written (just delete any partial last chunk file and re-run to resume).

    :param input_path: Path to the source `.doc.dump` (or any other newline-delimited
        JSON) file. Will be truncated in place as chunks are extracted.
    :param output_dir: Directory to write chunk files into. Defaults to the input
        file's own directory (must not be the same file as the input, obviously).
    :param chunk_size_bytes: Maximum size, in bytes, for each chunk file.
    :return: The list of chunk file paths written, in the order they were cut (from the
        tail of the input backwards - unrelated to their original order in the file).
    """
    if output_dir is None:
        output_dir = os.path.dirname(os.path.abspath(input_path))
    os.makedirs(output_dir, exist_ok=True)

    base_name = os.path.basename(input_path)
    chunk_paths = []
    chunk_index = 0

    while True:
        total_size = os.path.getsize(input_path)
        if total_size == 0:
            break

        target_cut = max(0, total_size - chunk_size_bytes)
        cut_point = _find_forward_line_boundary(input_path, target_cut)

        chunk_path = os.path.join(output_dir, f'{base_name}.part{chunk_index:03d}')
        _copy_range(input_path, chunk_path, cut_point, total_size)

        # Sanity check before touching the source: the chunk on disk must match what
        # we intended to remove, or we abort without truncating anything.
        if os.path.getsize(chunk_path) != total_size - cut_point:
            raise IOError(f'Chunk {chunk_path} size mismatch after write; aborting before truncating source.')

        with open(input_path, 'r+b') as f:
            f.truncate(cut_point)

        chunk_paths.append(chunk_path)
        chunk_index += 1
        print(f'  cut {chunk_path} ({(total_size - cut_point) / 1024 ** 3:.2f} GB); '
              f'{cut_point / 1024 ** 3:.2f} GB remaining in source')

        if cut_point == 0:
            break

    return chunk_paths


def main():
    parser = argparse.ArgumentParser(
        description='Split a line-separated JSON document dump file (e.g. a *.doc.dump '
                     'file from bioterms-cli --offline) into smaller size-bounded JSONL chunks.',
    )
    parser.add_argument('input_path', help='Path to the input .doc.dump / JSONL file.')
    parser.add_argument('--chunk-size-gb', type=float, default=50.0,
                         help='Maximum size of each output chunk, in GB (default: 50).')
    parser.add_argument('--output-dir', default=None,
                         help='Directory to write chunks into (default: same directory as input).')
    parser.add_argument('--in-place', action='store_true',
                         help='Shrink the input file as chunks are cut from its tail, so peak disk '
                              'usage is only input size + one chunk instead of ~2x the input. '
                              'DESTRUCTIVE and irreversible - see module docstring.')
    args = parser.parse_args()

    if not os.path.isfile(args.input_path):
        print(f'Input file not found: {args.input_path}', file=sys.stderr)
        sys.exit(1)

    chunk_size_bytes = int(args.chunk_size_gb * 1024 ** 3)

    if args.in_place:
        print(f'WARNING: --in-place will destructively truncate {args.input_path} as chunks are cut. '
              f'Press Ctrl+C now to abort.')
        chunk_paths = split_doc_dump_in_place(
            input_path=args.input_path,
            output_dir=args.output_dir,
            chunk_size_bytes=chunk_size_bytes,
        )
    else:
        chunk_paths = split_doc_dump(
            input_path=args.input_path,
            output_dir=args.output_dir,
            chunk_size_bytes=chunk_size_bytes,
        )

    print(f'Wrote {len(chunk_paths)} chunk(s):')
    for path in chunk_paths:
        size_gb = os.path.getsize(path) / 1024 ** 3
        print(f'  {path} ({size_gb:.2f} GB)')


if __name__ == '__main__':
    main()
