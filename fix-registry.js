// Collector-local registry. Trader's UI registry is deliberately unchanged in this correction.
const collectorFixRegistry = [
  {
    id: "FIX-132-OWNERSHIP",
    parentFix: "FIX-132",
    status: "IMPLEMENTED_DEFAULT_OFF",
    title: "Acquire collector lease before ingestion; drain before recovery/release",
    scope: "Collector only; no schema or Trader trading-rule changes",
    behavior: "Private one-connection ownership pool, atomic admission states, pure transactional fence, non-overlapping REST recovery, readiness and timing diagnostics",
    limitation: "Lease/lock/network uncertainty can still interrupt ingestion; global persistence lock remains; live load acceptance required",
    documentation: "md/FIX-132-ownership-correction.md",
    verification: "md/FIX-132-test-results.txt"
  }
];
