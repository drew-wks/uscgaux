import types
from unittest.mock import MagicMock

import pandas as pd
import pytest
from langchain.schema import Document

import langchain_utils


def test_init_vectorstore(monkeypatch):
    called = {}

    class FakeVS:
        def __init__(self, client, collection_name, embedding, validate_collection_config=True):
            called['client'] = client
            called['collection_name'] = collection_name
            called['embedding'] = embedding

    monkeypatch.setattr(langchain_utils, 'QdrantVectorStore', FakeVS)
    monkeypatch.setattr(langchain_utils, 'OpenAIEmbeddings', lambda model: f"emb:{model}")
    monkeypatch.setattr(langchain_utils, 'rag_config', lambda k: {'qdrant_collection_name':'col','embedding_model':'mod'}[k])

    client = MagicMock()
    vs = langchain_utils.init_vectorstore(client)
    assert called['client'] is client
    assert called['collection_name'] == 'col'
    assert vs is not None


def test_chunk_docs(monkeypatch):
    docs = [Document(page_content="a b c", metadata={})]

    class FakeSplitter:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def split_documents(self, docs_pages):
            return ["chunk1", "chunk2"]

    monkeypatch.setattr(langchain_utils, "RecursiveCharacterTextSplitter", FakeSplitter)
    cfg = {"chunk_size": 2, "chunk_overlap": 0, "length_function": len, "separators": [" "]}
    chunks = langchain_utils.chunk_Docs(docs, cfg)
    assert chunks == ["chunk1", "chunk2"]
