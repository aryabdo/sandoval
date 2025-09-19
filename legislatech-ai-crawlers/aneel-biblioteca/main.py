"""Ferramentas de ingestão para a Biblioteca da ANEEL.

Este módulo lê os artefatos produzidos pelo coletor headless
fornecido (resultados, índices e lista de controle) e exporta os
registros para a coleção vetorial utilizada pelo SANDOVAL.

Fluxo resumido:

1. leitura de ``lists/control_master.json`` e dos ``_index_<ano>.csv``;
2. enriquecimento com títulos, datas e inferências básicas;
3. extração de texto de PDFs e planilhas suportadas;
4. preparo dos metadados esperados pela API do SANDOVAL;
5. upsert na tabela ``langchain_pg_embedding`` usando pgvector.

O script assume que o coletor já foi executado e que os arquivos estão
organizados conforme o padrão::

    base_dir/
        results/<TIPO>/<ANO>/<arquivo>
        lists/control_master.json
        results/_index_<ANO>.csv

Variáveis de ambiente necessárias:
- ``OPENAI_API_KEY``
- ``POSTGRES_CONNECTION_STRING``
- ``ANEEL_BIBLIOTECA_DIR`` (opcional; caminho padrão da base local)
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import json
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pdfplumber
from dateparser.search import search_dates
from dotenv import load_dotenv
from langchain.schema import Document
from langchain_openai import OpenAIEmbeddings
from langchain_postgres import PGVector
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

LOGGER = logging.getLogger("aneel_biblioteca")
DEFAULT_COLLECTION = "michel_teste"
DEFAULT_FONTE = "Biblioteca ANEEL"
SUPPORTED_TEXT_TABLES = {".csv", ".tsv"}
SUPPORTED_PDFS = {".pdf"}
BINARY_EXTENSIONS = {
    ".zip",
    ".rar",
    ".7z",
    ".tar",
    ".gz",
    ".bz2",
    ".doc",
    ".docx",
    ".ppt",
    ".pptx",
    ".odt",
    ".odp",
    ".ods",
    ".xls",
    ".xlsx",
    ".xlsm",
}


@dataclass(slots=True)
class AneelEntry:
    """Representa um item listado em ``control_master.json``."""

    year: str
    tipo: str
    filename: str
    source_url: str
    pdf_url: Optional[str]
    mode: str
    collected_at: str
    title: Optional[str]
    local_path: Path


@dataclass(slots=True)
class PreparedDocument:
    """Documento pronto para ser enviado ao pgvector."""

    doc: Document
    checksum: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Exportador ANEEL Biblioteca")
    parser.add_argument(
        "--base-dir",
        type=Path,
        default=Path(os.getenv("ANEEL_BIBLIOTECA_DIR", Path.cwd())),
        help="Diretório que contém a estrutura 'results/' e 'lists/'.",
    )
    parser.add_argument(
        "--collection",
        type=str,
        default=DEFAULT_COLLECTION,
        help="Nome da coleção no PGVector (default: michel_teste)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Apenas mostra o que seria inserido, sem tocar no banco.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limita a quantidade de documentos processados.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Ativa logging em nível DEBUG.",
    )
    return parser.parse_args()


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="[%(levelname)s] %(message)s")


def load_master_entries(base_dir: Path) -> List[dict]:
    control_path = base_dir / "lists" / "control_master.json"
    if not control_path.exists():
        raise FileNotFoundError(
            f"Arquivo de controle não encontrado: {control_path}. "
            "Execute o coletor ANEEL antes de exportar."
        )
    data = json.loads(control_path.read_text(encoding="utf-8"))
    entries = data.get("entries", [])
    LOGGER.debug("Carregado control_master com %d entradas", len(entries))
    return entries


def build_title_lookup(results_dir: Path) -> Dict[str, str]:
    lookup: Dict[str, str] = {}
    for csv_path in results_dir.glob("_index_*.csv"):
        LOGGER.debug("Lendo índice: %s", csv_path)
        with csv_path.open("r", encoding="utf-8", newline="") as handler:
            reader = csv.DictReader(handler, delimiter=";")
            for row in reader:
                url = (row.get("URL_origem") or "").strip()
                title = (row.get("Titulo/Texto") or "").strip()
                if url and title:
                    lookup[url] = title
    LOGGER.debug("Mapa de títulos com %d itens", len(lookup))
    return lookup


def resolve_local_path(base_dir: Path, entry: dict) -> Path:
    results_dir = base_dir / "results"
    return results_dir / entry.get("tipo", "") / entry.get("year", "") / entry.get("fname", "")


def create_entries(base_dir: Path) -> Iterable[AneelEntry]:
    master_entries = load_master_entries(base_dir)
    title_lookup = build_title_lookup(base_dir / "results")

    for raw in master_entries:
        local_path = resolve_local_path(base_dir, raw)
        if not local_path.exists():
            LOGGER.warning("Arquivo %s não encontrado; pulando", local_path)
            continue
        title = title_lookup.get(raw.get("source_url", ""))
        yield AneelEntry(
            year=str(raw.get("year", "")),
            tipo=str(raw.get("tipo", "")),
            filename=str(raw.get("fname", "")),
            source_url=str(raw.get("source_url", "")),
            pdf_url=raw.get("pdf_url"),
            mode=str(raw.get("mode", "")),
            collected_at=str(raw.get("ts", "")),
            title=title,
            local_path=local_path,
        )


def extract_pdf_text(path: Path) -> str:
    text_fragments: List[str] = []
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            content = page.extract_text() or ""
            content = content.replace("\u0000", "").strip()
            if content:
                text_fragments.append(content)
    return "\n".join(text_fragments)


def extract_text_table(path: Path) -> str:
    content = path.read_text(encoding="utf-8", errors="ignore")
    if path.suffix.lower() == ".tsv":
        return content.replace("\t", ",")
    return content


def sanitise_text(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def infer_numero_norma(title: Optional[str]) -> Optional[str]:
    if not title:
        return None
    patterns = [
        r"n[ºo]\s*(\d+[\.\d]*)",
        r"no\s*(\d+[\.\d]*)",
        r"n\.\s*(\d+[\.\d]*)",
    ]
    for pattern in patterns:
        match = re.search(pattern, title, flags=re.IGNORECASE)
        if match:
            return match.group(1)
    return None


def infer_data_norma(title: Optional[str], texto: str) -> Optional[str]:
    bases = [title or "", texto[:2000]]
    for base in bases:
        if not base:
            continue
        results = search_dates(
            base,
            languages=["pt"],
            settings={"DATE_ORDER": "DMY", "RETURN_AS_TIMEZONE_AWARE": False},
        )
        if not results:
            continue
        for _, dt in results:
            if 1900 <= dt.year <= 2100:
                return dt.date().isoformat()
    return None


def infer_sancionador(texto: str) -> str:
    lowered = texto.lower()
    if "diretoria colegiada" in lowered:
        return "Diretoria Colegiada da ANEEL"
    if "diretor-geral" in lowered or "diretora-geral" in lowered:
        return "Diretor-Geral da ANEEL"
    return "Agência Nacional de Energia Elétrica"


def build_document(entry: AneelEntry) -> Optional[PreparedDocument]:
    suffix = entry.local_path.suffix.lower()
    try:
        if suffix in SUPPORTED_PDFS:
            raw_text = extract_pdf_text(entry.local_path)
        elif suffix in SUPPORTED_TEXT_TABLES:
            raw_text = extract_text_table(entry.local_path)
        else:
            LOGGER.warning("Extensão %s não suportada em %s", suffix, entry.local_path)
            return None
    except Exception as exc:  # pragma: no cover - leitura defensiva
        LOGGER.error("Falha ao extrair texto de %s: %s", entry.local_path, exc)
        return None

    if not raw_text:
        LOGGER.warning("Documento vazio: %s", entry.local_path)
        return None

    text = sanitise_text(raw_text)
    title = entry.title or entry.filename
    numero = infer_numero_norma(title)
    data_norma = infer_data_norma(title, text)
    sancionador = infer_sancionador(text)

    identifier_seed = f"{entry.source_url or entry.local_path}:{entry.year}:{entry.tipo}"
    checksum = hashlib.sha1(identifier_seed.encode("utf-8")).hexdigest()
    doc_id = f"aneel_biblioteca::{entry.year}::{entry.tipo}::{checksum[:12]}"

    metadata = {
        "id": doc_id,
        "fonte": DEFAULT_FONTE,
        "colecao": "aneel_biblioteca",
        "titulo": title,
        "ano": entry.year,
        "num_lei": numero,
        "data_lei": data_norma,
        "main_sancionador": sancionador,
        "tipo": entry.tipo,
        "link": entry.source_url,
        "pdf_url": entry.pdf_url,
        "arquivo_local": str(entry.local_path),
        "coletado_em": entry.collected_at,
        "modo_origem": entry.mode,
    }

    LOGGER.debug("Preparado documento %s", doc_id)
    return PreparedDocument(
        doc=Document(page_content=text, metadata=metadata),
        checksum=checksum,
    )


async def upsert_documents(
    engine: AsyncEngine,
    vector_store: PGVector,
    prepared: List[PreparedDocument],
    collection: str,
) -> None:
    if not prepared:
        LOGGER.info("Nenhum documento para inserir.")
        return

    doc_ids = [item.doc.metadata["id"] for item in prepared]
    LOGGER.info("Inserindo %d documentos na coleção '%s'", len(doc_ids), collection)
    async with engine.begin() as conn:
        for doc_id in doc_ids:
            await conn.execute(
                text(
                    "DELETE FROM langchain_pg_embedding "
                    "WHERE collection_name = :collection AND cmetadata->>'id' = :doc_id"
                ),
                {"collection": collection, "doc_id": doc_id},
            )

    await vector_store.aadd_documents(
        documents=[item.doc for item in prepared],
        ids=doc_ids,
    )


def prepare_documents(entries: Iterable[AneelEntry], limit: Optional[int] = None) -> List[PreparedDocument]:
    documents: List[PreparedDocument] = []
    skipped_binary = 0
    skipped_other = 0

    for entry in entries:
        if limit is not None and len(documents) >= limit:
            break

        suffix = entry.local_path.suffix.lower()
        if suffix in BINARY_EXTENSIONS:
            skipped_binary += 1
            LOGGER.info(
                "Ignorando arquivo binário %s (%s)",
                entry.local_path,
                suffix,
            )
            continue

        prepared = build_document(entry)
        if prepared is not None:
            documents.append(prepared)
        else:
            skipped_other += 1

    LOGGER.info(
        "Total de documentos preparados: %d | binários ignorados: %d | descartados por outros motivos: %d",
        len(documents),
        skipped_binary,
        skipped_other,
    )
    return documents


async def export(base_dir: Path, collection: str, dry_run: bool, limit: Optional[int]) -> None:
    entries = create_entries(base_dir)
    documents = prepare_documents(entries, limit)
    if dry_run:
        for doc in documents:
            LOGGER.info(
                "DRY RUN → %s (%s)",
                doc.doc.metadata.get("titulo"),
                doc.doc.metadata.get("id"),
            )
        LOGGER.info("Dry-run concluído sem inserir dados.")
        return

    load_dotenv()
    connection_string = os.getenv("POSTGRES_CONNECTION_STRING")
    openai_key = os.getenv("OPENAI_API_KEY")
    if not connection_string:
        raise RuntimeError("POSTGRES_CONNECTION_STRING não definido no ambiente")
    if not openai_key:
        raise RuntimeError("OPENAI_API_KEY não definido no ambiente")

    async_engine = create_async_engine(
        connection_string.replace("postgresql://", "postgresql+asyncpg://"),
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
    )
    embeddings = OpenAIEmbeddings(model="text-embedding-3-large")
    vector_store = PGVector(
        embeddings=embeddings,
        collection_name=collection,
        connection=async_engine,
        use_jsonb=True,
    )

    await upsert_documents(async_engine, vector_store, documents, collection)


def main() -> None:
    args = parse_args()
    configure_logging(args.verbose)
    base_dir = args.base_dir
    if not base_dir.exists():
        raise FileNotFoundError(f"Diretório base inexistente: {base_dir}")

    LOGGER.info("Exportando arquivos da ANEEL a partir de %s", base_dir)
    asyncio.run(export(base_dir, args.collection, args.dry_run, args.limit))


if __name__ == "__main__":
    main()
