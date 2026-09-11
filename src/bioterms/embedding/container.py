"""
Embedding container file formats and related classes.
"""

import struct
from abc import ABC, abstractmethod
from collections.abc import AsyncIterable, Iterable, AsyncIterator
from typing import Optional, Final
import aiofiles
import numpy as np

from bioterms.etc.enums import EmbeddingKind


_KIND_CODES: Final[dict[EmbeddingKind, int]] = {
    EmbeddingKind.ALIAS: 0,
    EmbeddingKind.DEFINITION: 1,
}
_KIND_FROM_CODE: Final[dict[int, EmbeddingKind]] = {v: k for k, v in _KIND_CODES.items()}


class EmbeddingContainer(ABC):
    """
    Abstract base class for embedding containers.
    """
    _VERSION: Optional[int] = None


class EmbeddingContainerV2(EmbeddingContainer):
    """
    Embedding container for version 2 format: one row per embedding item (a concept's label,
    a single synonym, or its definition -- see `Concept.embedding_items`), rather than one row
    per concept. Multiple rows share the same `concept_id` when a concept has several items.
    """
    _VERSION: Final[int] = 2

    def __init__(self,
                 item_id: str,
                 concept_id: str,
                 kind: EmbeddingKind,
                 text: str,
                 vector: np.ndarray,
                 ):
        """
        Initialise the EmbeddingContainerV2 instance.
        :param item_id: The identifier of the embedding item (see `EmbeddingItem.item_id`),
            used as the stable key for this vector in the vector database.
        :param concept_id: The identifier of the concept the item belongs to
        :param kind: Whether this item is an alias (label/synonym) or a definition
        :param text: The item's source text (see `EmbeddingItem.text`)
        :param vector: The embedding vector as a numpy array
        """
        self.item_id = item_id
        self.concept_id = concept_id
        self.kind = kind
        self.text = text
        self.vector = vector


class EmbeddingContainerFile(ABC):
    """
    Abstract base class for embedding container file formats.
    """
    _VERSION: Optional[int] = None
    _MAGIC: Optional[bytes] = None

    @staticmethod
    async def _read_exact(f,
                          n: int,
                          ) -> bytes:
        """
        aiofiles.read(n) may return fewer than n bytes; this reads exactly n or raises EOFError.
        :param f: The file object to read from
        :param n: The exact number of bytes to read
        :return: A bytes object containing exactly n bytes
        """
        buf = bytearray()
        while len(buf) < n:
            chunk = await f.read(n - len(buf))
            if not chunk:
                raise EOFError('Unexpected EOF')
            buf += chunk
        return bytes(buf)

    @staticmethod
    async def _aiter_from_maybe_async(containers: Iterable[EmbeddingContainer] | \
                                                  AsyncIterable[EmbeddingContainer]
                                      ) -> AsyncIterator[EmbeddingContainer]:
        """
        Convert an iterable or async iterable into an async iterator.
        :param containers: An iterable or async iterable of EmbeddingContainer instances
        :return: An asynchronous iterator of EmbeddingContainer instances
        """
        if hasattr(containers, '__aiter__'):
            async for x in containers:
                yield x
        else:
            for x in containers:
                yield x

    @staticmethod
    def _as_vec_f32(vector: np.ndarray| list[float]| tuple[float, ...],
                    dim: int,
                    ) -> np.ndarray:
        """
        Convert a vector to a contiguous float32 numpy array of the specified dimension.
        :param vector: The input vector
        :param dim: The expected dimensionality of the vector
        :return: A contiguous numpy array of type float32 and shape (dim,)
        """
        a = np.asarray(vector, dtype=np.float32)
        if a.shape != (dim,):
            raise ValueError(f'Vector shape {a.shape} != ({dim},)')

        return np.ascontiguousarray(a)

    @abstractmethod
    async def read(self) -> AsyncIterator[EmbeddingContainer]:
        """
        Read embedding containers from the file.
        :return: An asynchronous iterator of EmbeddingContainer instances.
        """

    @abstractmethod
    async def write(self,
                    containers: Iterable[EmbeddingContainer] | \
                                AsyncIterable[EmbeddingContainer],
                    ):
        """
        Write embedding containers to the file.
        :param containers: An iterable or async iterable of EmbeddingContainer instances to write
        """


class EmbeddingContainerFileV2(EmbeddingContainerFile):
    """
    Embedding container file format version 2 (one row per embedding item). The embedding
    vector dimension is written into the file header and read back, rather than assumed, so
    the file is self-describing regardless of which embedding model produced it.
    """
    _VERSION: Final[int] = 2
    _MAGIC: Final[bytes] = b'EMB2'
    _HDR_STRUCT: Final[struct.Struct] = struct.Struct('<4sH I H H')
    _BLK_HDR_STRUCT: Final[struct.Struct] = struct.Struct('<I')

    def __init__(self,
                 path: str,
                 *,
                 dim: int,
                 block_rows: int = 50000,
                 ):
        """
        Initialise the EmbeddingContainerFileV2 instance.
        :param path: The file path to read from or write to
        :param dim: The dimensionality of the embedding vectors (required when writing; when
            reading, the value from the file header is used instead)
        :param block_rows: The number of rows per block in the file
        """
        if dim <= 0:
            raise ValueError('dim must be a positive integer')
        if block_rows <= 0:
            raise ValueError('block_rows must be a positive integer')

        self.path = path
        self.dim = dim
        self.block_rows = block_rows

    async def _write_block(self,
                           f,
                           rows: list[EmbeddingContainerV2],
                           ) -> None:
        """
        Write a block of embedding containers to the file.
        :param f: The file object to write to
        :param rows: A list of EmbeddingContainerV2 instances to write
        """
        payload = bytearray()
        payload += struct.pack('<I', len(rows))  # n_rows

        for r in rows:
            item_id_bytes = r.item_id.encode()
            if len(item_id_bytes) > 0xFFFF:
                raise ValueError('item_id too long for u16 length')
            cid_bytes = r.concept_id.encode()
            if len(cid_bytes) > 0xFFFF:
                raise ValueError('concept_id too long for u16 length')
            text_bytes = r.text.encode()
            if len(text_bytes) > 0xFFFF:
                raise ValueError('text too long for u16 length')

            payload += struct.pack('<H', len(item_id_bytes))
            payload += item_id_bytes
            payload += struct.pack('<H', len(cid_bytes))
            payload += cid_bytes
            payload += struct.pack('<H', len(text_bytes))
            payload += text_bytes
            payload += struct.pack('<B', _KIND_CODES[r.kind])

            vec = self._as_vec_f32(r.vector, self.dim)  # ensures contiguous float32
            payload += vec.tobytes(order='C')

        await f.write(self._BLK_HDR_STRUCT.pack(len(payload)))
        await f.write(payload)

    @staticmethod
    def _read_row(mv: memoryview,
                 offset: int,
                 dim: int,
                 vec_bytes: int,
                 ) -> tuple[EmbeddingContainerV2, int]:
        """
        Parse a single embedding row from a block payload starting at the given offset.
        :param mv: The memoryview over the block payload.
        :param offset: The byte offset to start reading the row from.
        :param dim: The dimensionality of the embedding vector.
        :param vec_bytes: The number of bytes used to encode the embedding vector.
        :return: A tuple of the parsed EmbeddingContainerV2 and the offset after this row.
        """
        def read_str(off: int) -> tuple[str, int]:
            if off + 2 > len(mv):
                raise EOFError('Unexpected EOF while reading string length')
            length = struct.unpack_from('<H', mv, off)[0]
            off += 2
            if off + length > len(mv):
                raise EOFError('Unexpected EOF while reading string')
            value = bytes(mv[off:off + length]).decode()
            return value, off + length

        item_id, offset = read_str(offset)
        concept_id, offset = read_str(offset)
        text, offset = read_str(offset)

        if offset + 1 > len(mv):
            raise EOFError('Unexpected EOF while reading kind')
        kind = _KIND_FROM_CODE[mv[offset]]
        offset += 1

        if offset + vec_bytes > len(mv):
            raise EOFError('Unexpected EOF while reading vector')
        vec = np.frombuffer(
            mv[offset:offset + vec_bytes],
            dtype=np.float32,
            count=dim,
        ).copy()
        offset += vec_bytes

        return EmbeddingContainerV2(
            item_id=item_id,
            concept_id=concept_id,
            kind=kind,
            text=text,
            vector=vec,
        ), offset

    async def read(self) -> AsyncIterator[EmbeddingContainerV2]:
        """
        Read embedding containers from the file.
        :return: An asynchronous iterator of EmbeddingContainer instances.
        """
        async with aiofiles.open(self.path, mode='rb') as f:
            hdr = await self._read_exact(f, self._HDR_STRUCT.size)
            magic, ver, dim, flags, _pad = self._HDR_STRUCT.unpack(hdr)

            if magic != self._MAGIC:
                raise ValueError('Invalid file format (bad magic)')
            if ver != self._VERSION:
                raise ValueError(f'Unsupported version: {ver}')
            if flags != 0:
                raise ValueError(f'Unsupported flags: {flags}')

            self.dim = int(dim)
            vec_bytes = self.dim * 4  # float32

            while True:
                try:
                    blk_len_bytes = await self._read_exact(f, self._BLK_HDR_STRUCT.size)
                except EOFError:
                    return  # End of file reached

                (blk_len,) = self._BLK_HDR_STRUCT.unpack(blk_len_bytes)

                try:
                    payload = await self._read_exact(f, blk_len)
                except EOFError as e:
                    raise EOFError('Unexpected EOF while reading block payload') from e

                mv = memoryview(payload)
                if len(mv) < 4:
                    raise EOFError('Unexpected EOF while reading number of rows')

                n = struct.unpack_from('<I', mv, 0)[0]
                offset = 4

                for _ in range(n):
                    container, offset = self._read_row(mv, offset, self.dim, vec_bytes)
                    yield container

    async def write(self,
                    containers: Iterable[EmbeddingContainerV2] | \
                                AsyncIterable[EmbeddingContainerV2],
                    ):
        """
        Write embedding containers to the file.
        :param containers: An iterable or async iterable of EmbeddingContainer instances to write
        """
        async with aiofiles.open(self.path, mode='wb') as f:
            await f.write(self._HDR_STRUCT.pack(
                self._MAGIC,
                self._VERSION,
                self.dim,
                0,
                0,
            ))

            buf: list[EmbeddingContainerV2] = []

            async for c in self._aiter_from_maybe_async(containers):
                if not isinstance(c, EmbeddingContainerV2):
                    raise TypeError(f'Expected EmbeddingContainerV2, got {type(c)}')

                vec = self._as_vec_f32(c.vector, dim=self.dim)
                buf.append(EmbeddingContainerV2(
                    item_id=c.item_id,
                    concept_id=c.concept_id,
                    kind=c.kind,
                    text=c.text,
                    vector=vec,
                ))

                if len(buf) >= self.block_rows:
                    await self._write_block(f, buf)
                    buf.clear()

            if buf:
                await self._write_block(f, buf)
                buf.clear()

            await f.flush()
