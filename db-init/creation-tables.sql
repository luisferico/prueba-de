-- Tabla para almacenar las coordenadas originales del CSV
CREATE TABLE IF NOT EXISTS raw_coordinates (
    id SERIAL PRIMARY KEY,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed BOOLEAN DEFAULT FALSE,
    batch_id INTEGER,
    file_path VARCHAR(500) NOT NULL,
    CONSTRAINT valid_latitude CHECK (latitude BETWEEN -90 AND 90),
    CONSTRAINT valid_longitude CHECK (longitude BETWEEN -180 AND 180)
);

-- Tabla para almacenar la información completa de códigos postales
CREATE TABLE IF NOT EXISTS postcode_details (
    id SERIAL PRIMARY KEY,
    coordinate_id INTEGER REFERENCES raw_coordinates(id),
    postcode VARCHAR(10),
    quality INTEGER,
    eastings INTEGER,
    northings INTEGER,
    country VARCHAR(50),
    nhs_ha VARCHAR(100),
    longitude DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    european_electoral_region VARCHAR(100),
    primary_care_trust VARCHAR(100),
    region VARCHAR(100),
    lsoa VARCHAR(100),
    msoa VARCHAR(100),
    incode VARCHAR(4),
    outcode VARCHAR(4),
    parliamentary_constituency VARCHAR(100),
    admin_district VARCHAR(100),
    parish VARCHAR(100),
    admin_county VARCHAR(100),
    date_of_introduction VARCHAR(6),
    admin_ward VARCHAR(100),
    ced VARCHAR(100),
    ccg VARCHAR(100),
    nuts VARCHAR(100),
    pfa VARCHAR(100),
    distance DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_coordinate
        FOREIGN KEY(coordinate_id)
        REFERENCES raw_coordinates(id)
        ON DELETE CASCADE
);

-- Tabla para almacenar los códigos de referencia
CREATE TABLE IF NOT EXISTS postcode_codes (
    id SERIAL PRIMARY KEY,
    postcode_detail_id INTEGER REFERENCES postcode_details(id),
    admin_district VARCHAR(20),
    admin_county VARCHAR(20),
    admin_ward VARCHAR(20),
    parish VARCHAR(20),
    parliamentary_constituency VARCHAR(20),
    ccg VARCHAR(20),
    ccg_id VARCHAR(20),
    ced VARCHAR(20),
    nuts VARCHAR(20),
    lsoa VARCHAR(20),
    msoa VARCHAR(20),
    lau2 VARCHAR(20),
    pfa VARCHAR(20),
    CONSTRAINT fk_postcode_detail
        FOREIGN KEY(postcode_detail_id)
        REFERENCES postcode_details(id)
        ON DELETE CASCADE
);

-- Tabla para almacenar logs de errores
CREATE TABLE IF NOT EXISTS error_logs (
    id SERIAL PRIMARY KEY,
    coordinate_id INTEGER REFERENCES raw_coordinates(id),
    error_type VARCHAR(50),
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Índices para optimizar consultas frecuentes
CREATE INDEX idx_raw_coordinates_processed ON raw_coordinates(processed);
CREATE INDEX idx_raw_coordinates_batch ON raw_coordinates(batch_id);
CREATE INDEX idx_raw_coordinates_lat_long ON raw_coordinates(latitude, longitude);
CREATE INDEX idx_postcode_details_postcode ON postcode_details(postcode);
CREATE INDEX idx_postcode_details_region ON postcode_details(region);
CREATE INDEX idx_postcode_details_country ON postcode_details(country);
CREATE INDEX idx_postcode_details_lat_long ON postcode_details(latitude, longitude);
CREATE INDEX idx_error_logs_type ON error_logs(error_type);
