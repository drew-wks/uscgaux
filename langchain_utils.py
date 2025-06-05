#  Utilities for langchain

from env_config import RAG_CONFIG
import logging
from typing import List, Dict, Any
from langchain_openai import OpenAIEmbeddings
from langchain_qdrant import QdrantVectorStore
import pypdf
from langchain_community.document_loaders import PyPDFLoader
from io import BytesIO
from googleapiclient.discovery import Resource
from langchain.schema import Document
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.schema import Document



def init_vectorstore(client: QdrantClient, config: dict) -> QdrantVectorStore:
    """
    Initialize a LangChain QdrantVectorStore using a Qdrant client and config.

    Args:
        client (QdrantClient): The Qdrant client instance.
        config (dict): Dictionary containing Qdrant and embedding model configuration.

    Returns:
        QdrantVectorStore: Initialized LangChain vectorstore object.

    Raises:
        KeyError: If required keys are missing in the config.
        Exception: For all other initialization errors.
    """
    try:
        collection_name = RAG_CONFIG["qdrant_collection_name"]
        embedding_model = RAG_CONFIG["embedding_model"]
    except KeyError as e:
        logging.error(f"Missing required RAG_CONFIG key: {e}")
        raise

    try:
        embedding = OpenAIEmbeddings(model=embedding_model)
        vectorstore = QdrantVectorStore(
            client=client,
            collection_name=collection_name,
            embedding=embedding,
            validate_collection_config=True
        )
        logging.info(f"LangChain QdrantVectorStore initialized for collection '{collection_name}'.")
        return vectorstore

    except Exception as e:
        logging.error(f"Failed to initialize LangChain QdrantVectorStore: {e}")
        raise



def pdf_to_docs_via_drive(
    drive_client: Resource,
    file_id: str,
    planned_validated_metadata: Dict[str, Any]
) -> List[Document]:
    """
    Extracts text and metadata from a PDF in Google Drive and returns a list of LangChain page-level Document objects.

    Args:
        drive_client (Resource): Authenticated Google Drive client.
        file_id (str): ID of the PDF file in Google Drive.
        planned_validated_metadata (dict): Metadata to apply to each Document.

    Returns:
        List[Document]: List of page-level LangChain Document objects.
    """

    docs_pages = []

    try:
        # Download the PDF from Drive
        request = drive_client.files().get_media(fileId=file_id)
        file_stream = BytesIO()
        request.execute(fd=file_stream)
        file_stream.seek(0)

        # Load with PyPDFLoader
        loader = PyPDFLoader(file_stream)
        docs = loader.load()

        # Use pypdf to extract additional metadata
        file_stream.seek(0)
        reader = pypdf.PdfReader(file_stream)
        enriched_metadata = {
            'page_count': len(reader.pages),
        }
        planned_validated_metadata.update(enriched_metadata)
        planned_validated_metadata.pop('pdf_file_name', None)

        # Add metadata to each doc
        for doc in docs:
            doc.metadata.update(planned_validated_metadata)
            docs_pages.append(doc)

        logging.info(f"Processed file_id: {file_id} | Pages: {len(docs_pages)}")

    except Exception as e:
        logging.error(f"Failed to process PDF from Drive (file_id={file_id}): {e}")

    return docs_pages



def chunk_documents(
    docs_pages: List[Document],
    config: Dict[str, Any]
) -> List[Document]:
    """
    Splits full-page Document objects into smaller chunks using LangChain's RecursiveCharacterTextSplitter.

    Args:
        docs_pages (List[Document]): List of full-page LangChain Document objects.
        config (dict): Configuration dictionary with chunking parameters.

    Returns:
        List[Document]: List of chunked Document objects.
        
    Raises:
        Exception: If any error occurs during chunking.
    """
    try:
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=RAG_CONFIG["chunk_size"],
            chunk_overlap=RAG_CONFIG["chunk_overlap"],
            length_function=RAG_CONFIG["length_function"],
            separators=RAG_CONFIG["separators"]
        )

        docs_chunks = text_splitter.split_documents(docs_pages)
        logging.info(f"Chunked {len(docs_pages)} pages into {len(docs_chunks)} chunks.")
        return docs_chunks

    except Exception as e:
        logging.error(f"Failed to chunk documents: {e}")
        raise
