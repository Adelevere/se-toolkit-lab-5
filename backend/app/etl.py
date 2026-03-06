"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items — lab/task catalog
- GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)

Both require HTTP Basic Auth (email + password from settings).
"""

import httpx
from datetime import datetime
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlmodel import select

from app.settings import settings
from app.models.item import ItemRecord
from app.models.learner import Learner
from app.models.interaction import InteractionLog

# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------


async def fetch_items() -> list[dict]:
    """Fetch the lab/task catalog from the autochecker API."""
    async with httpx.AsyncClient() as client:
        url = f"{settings.autochecker_api_url}/api/items"
        response = await client.get(
            url,
            auth=(settings.autochecker_email, settings.autochecker_password)
        )
        
        if response.status_code != 200:
            raise Exception(f"Failed to fetch items: {response.status_code}")
        
        return response.json()

async def fetch_logs(since: datetime | None = None) -> list[dict]:
    """Fetch check results from the autochecker API."""
    all_logs = []
    current_since = since
    
    async with httpx.AsyncClient() as client:
        while True:
            params = {"limit": 500}
            if current_since:
                params["since"] = current_since.isoformat().replace("+00:00", "Z")
            
            url = f"{settings.autochecker_api_url}/api/logs"
            response = await client.get(
                url,
                params=params,
                auth=(settings.autochecker_email, settings.autochecker_password)
            )
            
            if response.status_code != 200:
                raise Exception(f"Failed to fetch logs: {response.status_code}")
            
            data = response.json()
            logs_batch = data["logs"]
            all_logs.extend(logs_batch)
            
            if not data["has_more"]:
                break
            
            if logs_batch:
                last_log = logs_batch[-1]
                current_since = datetime.fromisoformat(
                    last_log["submitted_at"].replace("Z", "+00:00")
                )
    
    return all_logs


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------


async def load_items(items: list[dict], session: AsyncSession) -> int:
    """Load items (labs and tasks) into the database."""
    new_count = 0
    lab_map = {}
    
    # Process labs first
    for item in items:
        if item["type"] == "lab":
            lab_short_id = item["lab"]
            lab_title = item["title"]
            
            stmt = select(ItemRecord).where(
                ItemRecord.type == "lab",
                ItemRecord.title == lab_title
            )
            existing = await session.execute(stmt)
            lab_item = existing.scalar_one_or_none()
            
            if not lab_item:
                lab_item = ItemRecord(
                    type="lab",
                    title=lab_title
                )
                session.add(lab_item)
                await session.flush()
                new_count += 1
            
            lab_map[lab_short_id] = lab_item
    for item in items:
        if item["type"] == "task":
            lab_short_id = item["lab"]
            task_title = item["title"]
            
            parent_lab = lab_map.get(lab_short_id)
            if not parent_lab:
                continue
            
            stmt = select(ItemRecord).where(
                ItemRecord.type == "task",
                ItemRecord.title == task_title,
                ItemRecord.parent_id == parent_lab.id
            )
            existing = await session.execute(stmt)
            task_item = existing.scalar_one_or_none()
            
            if not task_item:
                task_item = ItemRecord(
                    type="task",
                    title=task_title,
                    parent_id=parent_lab.id
                )
                session.add(task_item)
                new_count += 1
    
    await session.commit()
    return new_count


async def load_logs(
    logs: list[dict], items_catalog: list[dict], session: AsyncSession
) -> int:
    """Load interaction logs into the database."""
    new_count = 0
    
    # Build title lookup map
    title_map = {}
    for item in items_catalog:
        lab = item["lab"]
        task = item.get("task")
        title_map[(lab, task)] = item["title"]
    
    for log in logs:
        # Find or create learner
        stmt = select(Learner).where(Learner.external_id == log["student_id"])
        existing = await session.execute(stmt)
        learner = existing.scalar_one_or_none()
        
        if not learner:
            learner = Learner(
                external_id=log["student_id"],
                student_group=log["group"]
            )
            session.add(learner)
            await session.flush()
        
        # Find item with more precise query
        key = (log["lab"], log.get("task"))
        item_title = title_map.get(key)
        if not item_title:
            continue
        
        # Определяем тип и ищем точнее
        if log.get("task") is None:  # это лаба
            stmt = select(ItemRecord).where(
                ItemRecord.title == item_title,
                ItemRecord.type == "lab"
            )
        else:  # это таска
            # Сначала найдем родительскую лабу
            parent_lab_title = None
            for k, v in title_map.items():
                if k[0] == log["lab"] and k[1] is None:
                    parent_lab_title = v
                    break
            
            if parent_lab_title:
                parent_stmt = select(ItemRecord.id).where(
                    ItemRecord.title == parent_lab_title,
                    ItemRecord.type == "lab"
                )
                parent_result = await session.execute(parent_stmt)
                parent_id = parent_result.scalar_one_or_none()
                
                if parent_id:
                    stmt = select(ItemRecord).where(
                        ItemRecord.title == item_title,
                        ItemRecord.type == "task",
                        ItemRecord.parent_id == parent_id
                    )
                else:
                    continue
            else:
                continue
        
        existing = await session.execute(stmt)
        item = existing.scalar_one_or_none()
        
        if not item:
            continue
        
        # Check if interaction already exists
        stmt = select(InteractionLog).where(
            InteractionLog.external_id == log["id"]
        )
        existing = await session.execute(stmt)
        if existing.scalar_one_or_none():
            continue
        
        # Create new interaction
        interaction = InteractionLog(
            external_id=log["id"],
            learner_id=learner.id,
            item_id=item.id,
            kind="attempt",
            score=log["score"],
            checks_passed=log["passed"],
            checks_total=log["total"],
            created_at=datetime.fromisoformat(
                log["submitted_at"].replace("Z", "+00:00")
            )
        )
        session.add(interaction)
        new_count += 1
        
        if new_count % 100 == 0:
            await session.flush()

    await session.commit()
    return new_count

# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def sync(session: AsyncSession) -> dict:
    """Run the full ETL pipeline."""
    # Step 1: Fetch and load items
    items_catalog = await fetch_items()
    new_items = await load_items(items_catalog, session)
    print(f"Loaded {new_items} new items")
    
    # Step 2: Get last sync time
    stmt = select(InteractionLog.created_at).order_by(
        InteractionLog.created_at.desc()
    ).limit(1)
    result = await session.execute(stmt)
    last_sync = result.scalar_one_or_none()
    
    # Step 3: Fetch and load logs
    logs = await fetch_logs(since=last_sync)
    print(f"Fetched {len(logs)} logs from API")
    
    new_interactions = await load_logs(logs, items_catalog, session)
    
    # Step 4: Get total count
    stmt = select(InteractionLog)
    result = await session.execute(stmt)
    total_interactions = len(result.scalars().all())
    
    return {
        "new_records": new_interactions,
        "total_records": total_interactions
    }