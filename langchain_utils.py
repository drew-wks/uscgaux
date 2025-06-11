#  Utilities for langchain

import logging
from typing import List, Dict, Any
from io import BytesIO
import pandas as pd
from qdrant_client import QdrantClient
import pypdf
from langchain_community.document_loaders.unstructured import UnstructuredFileIOLoader
from langchain.schema import Document
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_openai import OpenAIEmbeddings
from langchain_qdrant import QdrantVectorStore
from googleapiclient.discovery import Resource

from env_config import rag_config


def init_vectorstore(client: QdrantClient) -> QdrantVectorStore:
    """
    Initialize a LangChain QdrantVectorStore using a Qdrant client and config.

    Args:
        client (QdrantClient): The Qdrant client instance.

    Returns:
        QdrantVectorStore: Initialized LangChain vectorstore object.

    Raises:
        KeyError: If required keys are missing in the config.
        Exception: For all other initialization errors.
    """
    try:
        collection_name = rag_config("qdrant_collection_name")
        embedding_model = rag_config("embedding_model")
    except KeyError as e:
        logging.error("Missing required RAG_CONFIG key: %s", e)
        raise

    try:
        embedding = OpenAIEmbeddings(model=embedding_model)
        vectorstore = QdrantVectorStore(
            client=client,
            collection_name=collection_name,
            embedding=embedding,
            validate_collection_config=True
        )
        logging.info("LangChain QdrantVectorStore initialized for collection '%s'.", collection_name)
        return vectorstore

    except Exception as e:
        logging.error("Failed to initialize LangChain QdrantVectorStore: %s", e)
        raise
    
    

def pdf_to_Docs_via_Drive(
    drive_client: Resource,
    file_id: str,
    metadata_df: pd.DataFrame
) -> List[Document]:
    docs_pages = []
    if metadata_df.shape[0] != 1:
        raise ValueError("metadata_df must contain exactly one row")
    metadata_dict = metadata_df.iloc[0].to_dict()

    try:
        # Download PDF into memory
        request = drive_client.files().get_media(fileId=file_id)
        file_bytes = BytesIO()
        request.execute(fd=file_bytes)
        file_bytes.seek(0)

        # Load via UnstructuredFileIOLoader
        loader = UnstructuredFileIOLoader(file_bytes, mode="elements")
        docs = loader.load()

        # Extract page count metadata via pypdf
        file_bytes.seek(0)
        reader = pypdf.PdfReader(file_bytes)
        metadata_dict["page_count"] = len(reader.pages)

        # Update metadata on each doc
        for doc in docs:
            doc.metadata.update(metadata_dict)
            docs_pages.append(doc)

        logging.info("Processed file_id: %s | Pages: %s", file_id, len(docs_pages))

    except Exception as e:
        logging.error("Failed to process PDF from Drive (file_id=%s): %s", file_id, e)

    return docs_pages



def chunk_documents(
    docs_pages: List[Document],
    RAG_CONFIG: Dict[str, Any]
) -> List[Document]:
    """
    Splits full-page Document objects into smaller chunks using LangChain's RecursiveCharacterTextSplitter.

    Args:
        docs_pages (List[Document]): List of full-page LangChain Document objects.
        RAG_CONFIG (dict): Configuration dictionary with chunking parameters.

    Returns:
        List[Document]: List of chunked Document objects.
        
    Raises:
        Exception: If any error occurs during chunking.
    """
    try:
        chunk_size=rag_config("chunk_size"),
        chunk_overlap=rag_config("chunk_overlap"),
        length_function=rag_config("length_function"),
        separators=rag_config("separators")
    except KeyError as e:
        logging.error("Missing required RAG_CONFIG key: %s", e)
        raise
   
    try:    
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            length_function=length_function,
            separators=separators
        )

        docs_chunks = text_splitter.split_documents(docs_pages)
        logging.info("Chunked %s pages into %s chunks.", len(docs_pages), len(docs_chunks))
        return docs_chunks

    except Exception as e:
        logging.error("Failed to chunk documents: %s", e)
        raise
