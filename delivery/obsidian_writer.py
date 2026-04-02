"""
delivery/obsidian_writer.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Writes DataBlitz narratives to a separate GitHub repo formatted as
an Obsidian vault. After each pipeline run, the narrative becomes a
structured markdown note with YAML frontmatter that Obsidian renders
beautifully with tags, links, and backlinks.

Setup (one time):
  1. Create a new GitHub repo: e.g. Raghav-hex/DataBlitz-Vault
  2. Set OBSIDIAN_VAULT_REPO=Raghav-hex/DataBlitz-Vault in .env
  3. Install obsidian-git plugin in Obsidian
  4. Point the plugin at that repo — notes appear automatically

The vault structure written:
  DataBlitz-Vault/
    Weekly Digests/
      2026-W13.md          ← main narrative
      2026-W12.md
    Country Notes/
      USA/2026-W13.md      ← country analyst report
      UK/2026-W13.md
      India/2026-W13.md
      Brazil/2026-W13.md
    Index.md               ← auto-updated with links to all weeks

Every note has:
  - YAML frontmatter (tags, week, countries, indicators, date)
  - Wikilinks to country notes [[USA/2026-W13]]
  - Backlinks work automatically in Obsidian graph view
"""

from __future__ import annotations

import json
import os
import base64
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

logger = logging.getLogger(__name__)

GITHUB_API = "https://api.github.com"


class ObsidianWriter:
    def __init__(self, vault_repo: str, github_token: str) -> None:
        """
        vault_repo: "username/DataBlitz-Vault"
        github_token: PAT with repo write access
        """
        self.repo   = vault_repo
        self.token  = github_token
        self.headers = {
            "Authorization": f"Bearer {github_token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

    def _gh_put(self, path: str, content: str, message: str) -> bool:
        """Create or update a file in the vault repo via GitHub API."""
        url = f"{GITHUB_API}/repos/{self.repo}/contents/{path}"

        # Check if file exists (need SHA to update)
        sha = None
        try:
            r = httpx.get(url, headers=self.headers, timeout=15)
            if r.status_code == 200:
                sha = r.json().get("sha")
        except Exception:
            pass

        body: dict[str, Any] = {
            "message": message,
            "content": base64.b64encode(content.encode("utf-8")).decode("ascii"),
        }
        if sha:
            body["sha"] = sha

        try:
            r = httpx.put(url, headers=self.headers, json=body, timeout=15)
            if r.status_code in (200, 201):
                logger.info(f"Obsidian: wrote {path}")
                return True
            else:
                logger.warning(f"Obsidian: failed to write {path}: {r.status_code} {r.text[:200]}")
                return False
        except Exception as exc:
            logger.warning(f"Obsidian: error writing {path}: {exc}")
            return False

    def write_narrative(self, narrative: dict, week_id: str) -> bool:
        """Write the full weekly narrative + country notes to the vault."""
        ok = True

        # 1. Main weekly digest note
        main_md = self._format_main_note(narrative, week_id)
        ok &= self._gh_put(
            f"Weekly Digests/{week_id}.md",
            main_md,
            f"DataBlitz: weekly digest {week_id}",
        )

        # 2. Per-country analyst notes (raw agent outputs if available)
        for country, analysis in (narrative.get("agent_analyses") or {}).items():
            country_md = self._format_country_note(country, analysis, narrative, week_id)
            ok &= self._gh_put(
                f"Country Notes/{country.upper()}/{week_id}.md",
                country_md,
                f"DataBlitz: {country.upper()} analysis {week_id}",
            )

        # 3. Update index
        index_md = self._format_index(narrative, week_id)
        ok &= self._gh_put("Index.md", index_md, f"DataBlitz: update index for {week_id}")

        return ok

    def _format_main_note(self, narrative: dict, week_id: str) -> str:
        meta   = narrative.get("meta", {})
        briefs = narrative.get("country_briefs", {})
        text   = narrative.get("main_narrative", "")
        date   = narrative.get("generated_at", "")[:10]
        countries = meta.get("countries", [])

        # Country wikilinks
        country_links = " | ".join(f"[[Country Notes/{c.upper()}/{week_id}|{c.upper()}]]" for c in countries)

        frontmatter = f"""---
title: "DataBlitz {week_id}"
date: {date}
week: "{week_id}"
tags: [datablitz, weekly-digest, {", ".join(countries)}]
countries: [{", ".join(countries)}]
indicators: {meta.get("indicators_total", 0)}
synthesizer: "{meta.get("synthesizer", "")}"
---

"""
        header = f"""# DataBlitz — {week_id}

**Countries:** {country_links}
**Indicators tracked:** {meta.get("indicators_total", "?")}
**Generated:** {date}

---

"""
        # Format alerts section if present
        alerts = meta.get("alerts", [])
        alert_block = ""
        if alerts:
            alert_block = "## ⚠ Threshold Alerts\n\n"
            for a in alerts:
                icon = "🔴" if a.get("level") == "critical" else "🟡"
                alert_block += f"> {icon} **{a.get('level','').upper()}** — {a.get('message','')}\n"
            alert_block += "\n---\n\n"

        # Country briefs section
        brief_block = "## Country Snapshots\n\n"
        for c, brief in briefs.items():
            brief_block += f"### [[Country Notes/{c.upper()}/{week_id}|{c.upper()}]]\n\n{brief}\n\n"

        return frontmatter + header + alert_block + text + "\n\n---\n\n" + brief_block

    def _format_country_note(self, country: str, analysis: str, narrative: dict, week_id: str) -> str:
        date = narrative.get("generated_at", "")[:10]
        meta = narrative.get("meta", {})
        provider = meta.get("analyst_providers", {}).get(country, "")

        frontmatter = f"""---
title: "{country.upper()} Analysis {week_id}"
date: {date}
week: "{week_id}"
country: "{country}"
tags: [datablitz, {country}, country-analysis]
analyst-provider: "{provider}"
parent: "[[Weekly Digests/{week_id}]]"
---

"""
        header = f"""# {country.upper()} — {week_id}

**Part of:** [[Weekly Digests/{week_id}]]

---

"""
        return frontmatter + header + analysis

    def _format_index(self, narrative: dict, week_id: str) -> str:
        date = narrative.get("generated_at", "")[:10]
        # Extract headline from narrative
        text = narrative.get("main_narrative", "")
        headline = ""
        for line in text.split("\n"):
            if line.startswith("## HEADLINE"):
                continue
            if headline == "" and line.strip() and not line.startswith("#"):
                headline = line.strip()
                break

        return f"""# DataBlitz — Vault Index

> AI-powered weekly global data digest. Updated automatically after each pipeline run.

## Latest: [[Weekly Digests/{week_id}]]

> {headline}

---

## All Weekly Digests

- [[Weekly Digests/{week_id}]] — {date}

*(Earlier weeks will appear here as more runs complete)*

---

*Generated by DataBlitz pipeline. Last updated: {date}*
"""


def write_to_obsidian(narrative_path: str) -> bool:
    """
    Main entry point. Called from push_to_kv.py or run_pipeline.py after pipeline.
    Reads narrative JSON, writes to Obsidian vault repo.
    """
    vault_repo    = os.environ.get("OBSIDIAN_VAULT_REPO", "")
    github_token  = os.environ.get("OBSIDIAN_GITHUB_TOKEN") or os.environ.get("GITHUB_TOKEN", "")

    if not vault_repo or not github_token:
        logger.info("Obsidian writer: OBSIDIAN_VAULT_REPO or token not set — skipping")
        return False

    try:
        narrative = json.loads(Path(narrative_path).read_text())
    except Exception as exc:
        logger.error(f"Obsidian writer: could not read narrative: {exc}")
        return False

    run_id  = narrative.get("run_id", "")
    week_id = datetime.now(tz=timezone.utc).strftime("%Y-W%V")
    if run_id:
        try:
            from datetime import datetime as dt
            parsed = dt.fromisoformat(run_id.replace("Z", "+00:00"))
            week_id = parsed.strftime("%Y-W%V")
        except Exception:
            pass

    writer = ObsidianWriter(vault_repo, github_token)
    ok = writer.write_narrative(narrative, week_id)

    if ok:
        print(f"  Obsidian ✓  vault updated: {vault_repo}  week={week_id}")
    else:
        print(f"  Obsidian ⚠  partial write — check logs")

    return ok
