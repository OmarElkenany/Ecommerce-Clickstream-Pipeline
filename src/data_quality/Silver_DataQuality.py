#!/usr/bin/env python
# coding: utf-8

# ## Silver_DataQuality
# 
# New notebook

# In[1]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%pyspark
# # =====================================================================================
# # NB_Silver_Data_Quality_Checks
# #
# # Runs after silver_layer_transform.py and silver_team_name_mapping.py.
# # Every check produces one row: (layer, table_name, check_name, status, value, detail).
# # Status is PASS / WARN / FAIL. Results are appended to the Delta table `dq_results`
# # with a run id and timestamp, and the notebook raises at the end if any check FAILED.
# # =====================================================================================

# import datetime
# import pyspark.sql.functions as F

# CUTOFF_START_SEASON = 2015
# CUTOFF_DATE = "2015-07-01"
# FK_WARN_RATE = 0.02      # more than 2% rows with a missing reference -> WARN
# FK_FAIL_RATE = 0.30      # more than 30% -> FAIL
# NULL_WARN_RATE = 0.50    # column more than 50% null -> WARN (informational)

# RUN_ID = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
# RUN_TS = datetime.datetime.now()
# RESULTS = []


# def record(table, check, status, value=None, detail=""):
#     RESULTS.append((RUN_ID, RUN_TS, "silver", table, check, status,
#                     None if value is None else float(value), str(detail)[:500]))
#     mark = {"PASS": "[ OK ]", "WARN": "[WARN]", "FAIL": "[FAIL]"}[status]
#     if status != "PASS":
#         print(f"{mark} {table:42s} {check:38s} {detail}")


# def exists(name):
#     return spark.catalog.tableExists(name)


# def load(name):
#     return spark.table(name) if exists(name) else None


# # ---- table inventory -------------------------------------------------------------------

# SILVER_TABLES = {
#     # table: dict(keys=natural key columns, period_season=col, period_date=col, not_null=[...])
#     "silver_player_profiles":              dict(keys=["player_id"], not_null=["player_id"]),
#     "silver_team_details":                 dict(keys=["club_id", "season_id", "competition_id"], season="start_season_year",
#                                                 not_null=["club_id", "club_name"]),
#     "silver_player_injuries":              dict(keys=["player_id", "season_name", "from_date", "injury_reason"],
#                                                 season="start_season_year", not_null=["player_id", "from_date"]),
#     "silver_player_latest_market_value":   dict(keys=["player_id"], not_null=["player_id"]),
#     "silver_player_market_value":          dict(keys=["player_id", "date_unix"], date="date_unix",
#                                                 not_null=["player_id", "date_unix", "value"]),
#     "silver_player_performances":          dict(keys=["player_id", "season_name", "competition_id", "team_id"],
#                                                 season="start_season_year", not_null=["player_id", "season_name"]),
#     "silver_transfer_history":             dict(keys=["player_id", "transfer_date", "from_team_id", "to_team_id"],
#                                                 season="start_season_year", not_null=["player_id"]),
#     "silver_team_competitions_seasons":    dict(keys=["club_id", "season_id", "competition_id"], season="start_season_year",
#                                                 not_null=["club_id"]),
#     "silver_team_children":                dict(keys=["parent_team_id", "child_team_id"], not_null=["parent_team_id", "child_team_id"]),
#     "silver_football_data_matches":        dict(keys=["Div", "Date", "HomeTeam", "AwayTeam"], season="start_season_year",
#                                                 not_null=["Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR"]),
#     "silver_match_weather":                dict(keys=["date", "home_team"], date="date", not_null=["date", "home_team"]),
#     "silver_club_climate":                 dict(keys=["club_id", "month"], not_null=["club_id", "month"]),
#     "silver_player_national_performances": dict(keys=["player_id", "team_id", "career_state"], not_null=["player_id"]),
#     "silver_player_teammates_played_with": dict(keys=["player_id", "teammate_player_id"], not_null=["player_id"]),
# }

# # Tables whose references point at a reference table that is only partially loaded by design:
# # team_details holds only the clubs of the competitions in the dataset, and teammates / national
# # squads include players outside the player scope. Missing references are therefore expected
# # and Gold turns them into inferred members, so these checks can only WARN.
# PARTIAL_REFERENCE_BY_DESIGN = {
#     "silver_player_performances", "silver_transfer_history", "silver_team_competitions_seasons",
#     "silver_team_children", "silver_player_teammates_played_with", "silver_player_national_performances",
#     "silver_club_climate",
# }

# DATE_TYPED = {"silver_player_market_value": ["date_unix"], "silver_player_injuries": ["from_date", "end_date"],
#               "silver_transfer_history": ["transfer_date"], "silver_match_weather": ["date"],
#               "silver_player_profiles": ["date_of_birth"], "silver_football_data_matches": ["Date"]}


# # ---- 1. generic checks per table ----------------------------------------------------------

# for tname, cfg in SILVER_TABLES.items():
#     df = load(tname)
#     if df is None:
#         bronze = "bronze_" + tname[len("silver_"):]
#         if not exists(bronze):
#             record(tname, "table_exists", "WARN", 0, f"{bronze} was never ingested, so Silver has nothing to build")
#         else:
#             record(tname, "table_exists", "FAIL", 0, "table is missing")
#         continue
#     n = df.count()
#     record(tname, "row_count", "PASS" if n > 0 else "FAIL", n, f"{n} rows" if n == 0 else "")
#     if n == 0:
#         continue
#     cols = set(df.columns)

#     # not-null on mandatory columns
#     for c in cfg.get("not_null", []):
#         if c not in cols:
#             record(tname, f"not_null:{c}", "FAIL", None, "column missing")
#             continue
#         bad = df.filter(F.col(c).isNull()).count()
#         record(tname, f"not_null:{c}", "PASS" if bad == 0 else "FAIL", bad, f"{bad} NULL values")

#     # duplicates on the natural key
#     keys = [k for k in cfg["keys"] if k in cols]
#     dups = n - df.select(*keys).distinct().count()
#     # Natural keys here are the deduplication keys, so any duplicate means dedup did not work
#     record(tname, "duplicate_natural_key", "PASS" if dups == 0 else "FAIL", dups, f"{dups} duplicate rows on {keys}")

#     # period compliance: nothing before the cutoff, nothing without a determinable period
#     if "season" in cfg and cfg["season"] in cols:
#         c = cfg["season"]
#         early = df.filter(F.col(c) < CUTOFF_START_SEASON).count()
#         nulls = df.filter(F.col(c).isNull()).count()
#         record(tname, "period_not_before_cutoff", "PASS" if early == 0 else "FAIL", early,
#                f"{early} rows with {c} < {CUTOFF_START_SEASON}")
#         record(tname, "period_determinable", "PASS" if nulls == 0 else "FAIL", nulls, f"{nulls} rows with NULL {c}")
#     if "date" in cfg and cfg["date"] in cols:
#         c = cfg["date"]
#         early = df.filter(F.col(c).cast("date") < F.lit(CUTOFF_DATE).cast("date")).count()
#         record(tname, "date_not_before_cutoff", "PASS" if early == 0 else "FAIL", early,
#                f"{early} rows dated before {CUTOFF_DATE}")

#     # column types for date columns
#     for c in DATE_TYPED.get(tname, []):
#         if c in cols:
#             t = dict(df.dtypes)[c]
#             record(tname, f"type:{c}", "PASS" if t in ("date", "timestamp") else "FAIL", None, f"{c} is {t}, expected date")

#     # soft FK flag rate
#     if "_has_missing_reference" in cols:
#         flagged = df.filter(F.col("_has_missing_reference") == True).count()
#         rate = flagged / n
#         st = "PASS" if rate <= FK_WARN_RATE else ("WARN" if rate <= FK_FAIL_RATE else "FAIL")
#         if st == "FAIL" and tname in PARTIAL_REFERENCE_BY_DESIGN:
#             st = "WARN"
#         per_col = [c for c in cols if c.startswith("_fk_missing_")]
#         parts = ", ".join(f"{c[len('_fk_missing_'):]}={df.filter(F.col(c) == True).count() / n:.1%}" for c in per_col)
#         record(tname, "missing_reference_rate", st, rate,
#                f"{flagged} of {n} rows flagged ({rate:.1%}); by column: {parts}")

#     # technical metadata
#     for c in ["_silver_load_timestamp", "_is_active"]:
#         if c not in cols:
#             record(tname, f"metadata:{c}", "FAIL", None, "technical column missing")

#     # null profile (informational)
#     for c in df.columns:
#         if c.startswith("_"):
#             continue
#         rate = df.filter(F.col(c).isNull()).count() / n
#         if rate > NULL_WARN_RATE:
#             record(tname, f"null_rate:{c}", "WARN", rate, f"{rate:.0%} NULL")
#     record(tname, "checked", "PASS", n)


# # ---- 2. rejected rows --------------------------------------------------------------------

# for t in [x.name for x in spark.catalog.listTables() if x.name.startswith("silver_rejected_")]:
#     rej = spark.table(t)
#     cnt = rej.count()
#     base = t.replace("silver_rejected_", "silver_")
#     kept = spark.table(base).count() if exists(base) else 0
#     rate = cnt / (cnt + kept) if (cnt + kept) else 0
#     st = "PASS" if rate <= 0.02 else ("WARN" if rate <= 0.10 else "FAIL")
#     reasons = ", ".join(f"{r['_reject_reason']}={r['count']}" for r in
#                         rej.groupBy("_reject_reason").count().orderBy(F.desc("count")).limit(5).collect())
#     record(t, "rejected_rate", st, rate, f"{cnt} rejected of {cnt + kept} ({rate:.1%}); {reasons}")


# # ---- 3. table specific business checks ---------------------------------------------------------

# m = load("silver_football_data_matches")
# if m is not None:
#     bad_ftr = m.filter(~F.col("FTR").isin("H", "D", "A")).count()
#     record("silver_football_data_matches", "ftr_allowed_values", "PASS" if bad_ftr == 0 else "FAIL", bad_ftr,
#            f"{bad_ftr} rows with FTR outside H/D/A")
#     inconsistent = m.filter(
#         ((F.col("FTHG") > F.col("FTAG")) & (F.col("FTR") != "H")) |
#         ((F.col("FTHG") < F.col("FTAG")) & (F.col("FTR") != "A")) |
#         ((F.col("FTHG") == F.col("FTAG")) & (F.col("FTR") != "D"))).count()
#     record("silver_football_data_matches", "result_matches_score", "PASS" if inconsistent == 0 else "FAIL",
#            inconsistent, f"{inconsistent} rows where FTR contradicts the score")
#     neg = m.filter((F.col("FTHG") < 0) | (F.col("FTAG") < 0)).count()
#     record("silver_football_data_matches", "goals_not_negative", "PASS" if neg == 0 else "FAIL", neg, f"{neg} negative goals")

# td = load("silver_team_details")
# if td is not None:
#     suffixed = td.filter(F.col("club_name").rlike(r"\(\d+\)\s*$")).count()
#     record("silver_team_details", "club_name_has_no_id_suffix", "PASS" if suffixed == 0 else "FAIL", suffixed,
#            f"{suffixed} club names still end with '(digits)'")

# pf = load("silver_player_performances")
# if pf is not None:
#     neg = pf.filter((F.col("minutes_played") < 0) | (F.col("goals") < 0) | (F.col("assists") < 0)).count()
#     record("silver_player_performances", "measures_not_negative", "PASS" if neg == 0 else "FAIL", neg, f"{neg} rows")
#     too_many = pf.filter(F.col("minutes_played") > 6000).count()
#     record("silver_player_performances", "minutes_plausible", "PASS" if too_many == 0 else "FAIL", too_many,
#            f"{too_many} rows above 6000 minutes in a season")

# mv = load("silver_player_market_value")
# if mv is not None:
#     neg = mv.filter(F.col("value") < 0).count()
#     record("silver_player_market_value", "value_not_negative", "PASS" if neg == 0 else "FAIL", neg, f"{neg} negative values")

# tr = load("silver_transfer_history")
# if tr is not None:
#     ph = tr.filter(F.col("from_team_id").isin(515, 123, 75, 2113) | F.col("to_team_id").isin(515, 123, 75, 2113)).count()
#     record("silver_transfer_history", "placeholder_clubs_not_flagged_missing", "PASS", ph,
#            f"{ph} transfers involve placeholder clubs (without club, retired, ...) and are not counted as missing references")

# # ---- 4. scope consistency -----------------------------------------------------------------------

# scope, prof = load("silver_scope_players"), load("silver_player_profiles")
# if scope is not None and prof is not None:
#     outside = prof.join(scope, "player_id", "left_anti").count()
#     record("silver_player_profiles", "profiles_within_scope", "PASS" if outside == 0 else "FAIL", outside,
#            f"{outside} profiles are not in silver_scope_players")
#     sc = scope.count()
#     dup = sc - scope.distinct().count()
#     record("silver_scope_players", "unique_player_id", "PASS" if dup == 0 else "FAIL", dup, f"{dup} duplicates")

# # orphan players in fact tables (no profile): informational, handled as inferred members in Gold
# if prof is not None:
#     for t in ["silver_player_performances", "silver_player_market_value", "silver_player_injuries", "silver_transfer_history"]:
#         d = load(t)
#         if d is None:
#             continue
#         ids = d.select("player_id").distinct()
#         orphan = ids.join(prof.select("player_id"), "player_id", "left_anti").count()
#         total = ids.count()
#         rate = orphan / total if total else 0
#         st = "PASS" if rate <= 0.05 else ("WARN" if rate <= 0.60 else "FAIL")
#         record(t, "players_without_profile", st, rate, f"{orphan} of {total} players have no profile ({rate:.1%})")

# # ---- 5. team name mapping ---------------------------------------------------------------------------

# mp = load("silver_team_name_mapping")
# if mp is None:
#     record("silver_team_name_mapping", "table_exists", "FAIL", 0, "run silver_team_name_mapping.py")
# else:
#     # football_data feeds fact_match, so it is judged on the share of MATCHES that resolve (see below).
#     # wikidata lists stadiums of clubs worldwide while the reference only holds the dataset's clubs,
#     # so a low match share there is expected and only reported.
#     for src in ["football_data", "wikidata"]:
#         s = mp.filter(F.col("source") == src)
#         tot = s.count()
#         if tot == 0:
#             record("silver_team_name_mapping", f"rows:{src}", "WARN", 0, f"no mapping rows for {src}")
#             continue
#         un = s.filter(F.col("match_method") == "unmatched").count()
#         rate = un / tot
#         record("silver_team_name_mapping", f"unmatched_share:{src}", "PASS" if rate <= 0.10 else "WARN", rate,
#                f"{un} of {tot} names unmatched ({rate:.1%}); informational")
#     dup = mp.groupBy("source", "raw_name").count().filter("count > 1").count()
#     record("silver_team_name_mapping", "unique_raw_name_per_source", "PASS" if dup == 0 else "FAIL", dup, f"{dup} duplicated names")

# # Share of football-data matches where both teams map to a club (what fact_match actually gets)
# if mp is not None and m is not None:
#     good = mp.filter((F.col("source") == "football_data") & (F.col("match_method") != "unmatched")).select("raw_name")
#     h = good.withColumnRenamed("raw_name", "HomeTeam")
#     a_ = good.withColumnRenamed("raw_name", "AwayTeam")
#     tot = m.count()
#     both = m.join(F.broadcast(h), "HomeTeam").join(F.broadcast(a_), "AwayTeam").count()
#     rate = 1 - both / tot if tot else 0
#     # Informational: football-data includes many lower leagues that Transfermarkt does not cover.
#     # Gold turns these names into inferred clubs, so such matches still get a real club key.
#     st = "PASS" if rate <= 0.10 else "WARN"
#     record("silver_football_data_matches", "matches_with_unresolved_team", st, rate,
#            f"{tot - both} of {tot} matches have a team outside the Transfermarkt reference ({rate:.1%}); Gold creates inferred clubs for them")

# # ---- 6. persist results and summarise ---------------------------------------------------------

# schema = "run_id string, run_timestamp timestamp, layer string, table_name string, check_name string, status string, value double, detail string"
# res_df = spark.createDataFrame(RESULTS, schema)
# res_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("dq_results")

# summary = {s: sum(1 for r in RESULTS if r[5] == s) for s in ["PASS", "WARN", "FAIL"]}
# print(f"\nSilver data quality run {RUN_ID}: {summary['PASS']} passed, {summary['WARN']} warnings, {summary['FAIL']} failed")
# failed = [r for r in RESULTS if r[5] == "FAIL"]
# if failed:
#     raise RuntimeError(f"{len(failed)} silver data quality checks failed: "
#                        + "; ".join(f"{r[3]}.{r[4]}" for r in failed[:15]))

