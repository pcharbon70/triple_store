labels =
  ~w(writes 1 2a 2b 2m 3 4 5 6 7 8a 8b 8m 9 10a 10b 11 12 13 14a 14b 15a 15b 16a 16b 17 18 19a 19b 20a 20b 20m)

power_records =
  labels
  |> Enum.with_index()
  |> Enum.map(fn
    {"writes", 0} ->
      %{batch_type: :power, day: ~D[2012-11-29], label: "writes", duration_s: 32.0}

    {label, index} ->
      %{batch_type: :power, day: ~D[2012-11-29], label: label, duration_s: index * 1.0}
  end)

throughput_records = [
  %{batch_type: :throughput, day: ~D[2012-11-30], label: "writes", duration_s: 1_000.0},
  %{batch_type: :throughput, day: ~D[2012-11-30], label: "reads", duration_s: 900.0},
  %{batch_type: :throughput, day: ~D[2012-12-01], label: "writes", duration_s: 1_100.0},
  %{batch_type: :throughput, day: ~D[2012-12-01], label: "reads", duration_s: 1_000.0}
]

records =
  Enum.map(power_records ++ throughput_records, fn record ->
    Map.merge(record, %{correct?: true, complete?: true, score_eligible?: true})
  end)

%{
  schema_version: 1,
  source: %{
    source_id: "snb-bi-1.0.3",
    source_commit: "5f7967235593eefa98b336e240039780b6a76a9a",
    scorer_path: "scoring/calculate-scores.py",
    scorer_sha256: "02086291b5732f0a96de4af870b707653a76f528d01e56d33e57635c1929ebbe",
    generator: "upstream calculate-scores.py with DuckDB 1.4.1"
  },
  inputs: %{
    scale_factor: 10,
    load_time_s: 3_600.0,
    throughput_min_s: 3_600.0,
    timings_sha256: "9acf884e890e9360df6e492de475ddbf695be0b2a65bfbee420fd208956f828b",
    load_sha256: "3a409294917f5b7d471e23c30b460170c4e62f89f41aaeeeb053c7faa336eede",
    benchmark_sha256: "2db7cdba1d2bc5c8448ff52fca395c195e9a6d243ea173ce1c4b3ef218468304"
  },
  expected: %{
    power: 278.17453399866326,
    power_at_scale: 2_781.7453399866326,
    throughput: 41.4,
    throughput_at_scale: 414.0
  },
  records: records
}
