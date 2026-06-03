import time
import threading
from sentence_transformers import SentenceTransformer

from bioterms.etc.utils import get_transformer
from bioterms.etc.metrics import EMBED_LOCK_WAIT, EMBED_DURATION, EMBED_TEXTS, EMBED_CHARS, \
    EMBED_ERRORS


class TextTransformer:
    """
    A class for transforming text into embeddings using a SentenceTransformer model.
    """
    _embed_lock = threading.Lock()

    def __init__(self,
                 transformer: SentenceTransformer = None,
                 ):
        """
        Initialise the TextTransformer with an optional SentenceTransformer.
        :param transformer: The SentenceTransformer instance to use for embedding; if None,
            the default transformer will be used
        """
        if transformer is None:
            self.managed = False
            self.transformer = get_transformer()
        else:
            self.managed = True
            self.transformer = transformer

    def embed_strings(self,
                      texts: list[str],
                      ) -> list[list[float]]:
        """
        Embed a list of strings using the provided SentenceTransformer.
        :param texts: The list of strings to embed
        :return: A list of embeddings, each represented as a list of floats,
            in the same order as the input texts
        """
        model = self.transformer.model_card_data.base_model \
                if self.transformer.model_card_data \
                else 'custom'
        EMBED_TEXTS.labels(model=model).observe(len(texts))
        EMBED_CHARS.labels(model=model).observe(sum(len(t) for t in texts))

        def encode_with_metrics():
            enc_start = time.perf_counter()
            vs = self.transformer.encode(
                inputs=texts,
                normalize_embeddings=True,
            )
            enc_end = time.perf_counter()
            EMBED_DURATION.labels(model=model, result='ok').observe(enc_end - enc_start)
            return vs

        wait_start = time.perf_counter()
        try:
            if not self.managed:
                with self._embed_lock:
                    wait_end = time.perf_counter()
                    EMBED_LOCK_WAIT.labels(model=model).observe(wait_end - wait_start)

                    vectors = encode_with_metrics()
            else:
                # Assume the provided transformer is thread-safe or already managed
                vectors = encode_with_metrics()
        except Exception as e:
            EMBED_DURATION.labels(model=model, result='error').observe(0.0)
            EMBED_ERRORS.labels(model=model, error_type=type(e).__name__).inc()
            raise

        return [vector.tolist() for vector in vectors]
