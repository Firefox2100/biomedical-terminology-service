"""
Embedding container file formats and related classes.
"""

import struct
from abc import ABC, abstractmethod
from collections.abc import AsyncIterable, Iterable, AsyncIterator
from uuid import UUID, uuid4
from typing import Optional, Final
import aiofiles
import numpy as np


class EmbeddingContainer(ABC):
    """
    Abstract base class for embedding containers.
    """
    _VERSION: Optional[int] = None


class EmbeddingContainerV1(EmbeddingContainer):
    """
    Embedding container for version 1 format.
    """
    _VERSION: Final[int] = 1

    def __init__(self,
                 concept_id: str,
                 vector: np.ndarray,
                 vector_id: UUID = None,
                 ):
        """
        Initialise the EmbeddingContainerV1 instance.
        :param concept_id: The concept identifier
        :param vector_id: The UUID of the vector
        :param vector: The embedding vector as a numpy array
        """
        self.concept_id = concept_id
        self.vector_id = vector_id or uuid4()
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
                    dim: int = 768,
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


class EmbeddingContainerFileV1(EmbeddingContainerFile):
    """
    Embedding container file format version 1.
    """
    _VERSION: Final[int] = 1
    _MAGIC: Final[bytes] = b'EMB1'
    _HDR_STRUCT: Final[struct.Struct] = struct.Struct('<4sH I H H')
    _BLK_HDR_STRUCT: Final[struct.Struct] = struct.Struct('<I')

    def __init__(self,
                 path: str,
                 *,
                 dim: int = 768,
                 block_rows: int = 50000,
                 ):
        """
        Initialise the EmbeddingContainerFileV1 instance.
        :param path: The file path to read from or write to
        :param dim: The dimensionality of the embedding vectors
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
                           rows: list[EmbeddingContainerV1],
                           ) -> None:
        """
        Write a block of embedding containers to the file.
        :param f: The file object to write to
        :param rows: A list of EmbeddingContainerV1 instances to write
        """
        payload = bytearray()
        payload += struct.pack('<I', len(rows))  # n_rows

        for r in rows:
            cid_bytes = r.concept_id.encode()
            if len(cid_bytes) > 0xFFFF:
                raise ValueError('concept_id too long for u16 length')

            payload += struct.pack('<H', len(cid_bytes))
            payload += cid_bytes
            payload += r.vector_id.bytes

            vec = self._as_vec_f32(r.vector, self.dim)  # ensures contiguous float32
            payload += vec.tobytes(order='C')

        await f.write(self._BLK_HDR_STRUCT.pack(len(payload)))
        await f.write(payload)

    async def read(self) -> AsyncIterator[EmbeddingContainerV1]:
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
                    if offset + 2 > len(mv):
                        raise EOFError('Unexpected EOF while reading concept_id length')
                    l1 = struct.unpack_from('<H', mv, offset)[0]
                    offset += 2
                    if offset + l1 > len(mv):
                        raise EOFError('Unexpected EOF while reading concept_id')
                    concept_id = bytes(mv[offset:offset + l1]).decode()
                    offset += l1

                    if offset + 16 > len(mv):
                        raise EOFError('Unexpected EOF while reading vector_id')
                    vector_id = UUID(bytes=bytes(mv[offset:offset + 16]))
                    offset += 16

                    if offset + vec_bytes > len(mv):
                        raise EOFError('Unexpected EOF while reading vector')
                    vec = np.frombuffer(
                        mv[offset:offset + vec_bytes],
                        dtype=np.float32,
                        count=self.dim,
                    ).copy()
                    offset += vec_bytes

                    yield EmbeddingContainerV1(
                        concept_id=concept_id,
                        vector_id=vector_id,
                        vector=vec,
                    )

    async def write(self,
                    containers: Iterable[EmbeddingContainerV1] | \
                                AsyncIterable[EmbeddingContainerV1],
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

            buf: list[EmbeddingContainerV1] = []

            async for c in self._aiter_from_maybe_async(containers):
                if not isinstance(c, EmbeddingContainerV1):
                    raise TypeError(f'Expected EmbeddingContainerV1, got {type(c)}')

                vec = self._as_vec_f32(c.vector, dim=self.dim)
                buf.append(EmbeddingContainerV1(
                    concept_id=c.concept_id,
                    vector_id=c.vector_id,
                    vector=vec,
                ))

                if len(buf) >= self.block_rows:
                    await self._write_block(f, buf)
                    buf.clear()

            if buf:
                await self._write_block(f, buf)
                buf.clear()

            await f.flush()
