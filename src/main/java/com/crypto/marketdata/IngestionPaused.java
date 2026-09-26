package com.crypto.marketdata;
/** Expected admission rejection; no candle transaction has been attempted. */
final class IngestionPaused extends RuntimeException {
    IngestionPaused(){super("FIX-132 ingestion admission closed");}
}
