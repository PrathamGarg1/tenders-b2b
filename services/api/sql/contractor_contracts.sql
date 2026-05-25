CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS contractor_contracts (
    contract_no text PRIMARY KEY,
    list_date text,
    product_name text,
    contract_value numeric,
    seller_name text,
    seller_email text,
    seller_phone text,
    seller_emails text[],
    seller_phones text[],
    seller_gstin text,
    seller_address text,
    source_pdf_sha256 text,
    product_embedding vector(1536),
    quality_flags text[],
    reject_reasons text[],
    is_reject boolean DEFAULT false,
    raw_record jsonb NOT NULL,
    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS contractor_contracts_product_embedding_idx
ON contractor_contracts USING ivfflat (product_embedding vector_cosine_ops)
WITH (lists = 100);

CREATE INDEX IF NOT EXISTS contractor_contracts_product_name_idx
ON contractor_contracts USING gin (to_tsvector('english', coalesce(product_name, '')));

CREATE INDEX IF NOT EXISTS contractor_contracts_seller_name_idx
ON contractor_contracts USING gin (to_tsvector('english', coalesce(seller_name, '')));

CREATE INDEX IF NOT EXISTS contractor_contracts_list_date_idx
ON contractor_contracts (list_date);

CREATE INDEX IF NOT EXISTS contractor_contracts_seller_gstin_idx
ON contractor_contracts (seller_gstin)
WHERE seller_gstin IS NOT NULL;

CREATE INDEX IF NOT EXISTS contractor_contracts_contract_value_idx
ON contractor_contracts (contract_value DESC)
WHERE contract_value IS NOT NULL;
