"""Analytics endpoints for the learning management service."""

from fastapi import APIRouter, Depends
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlmodel import select, func
from sqlalchemy import cast, Date

from app.database import get_session
from app.models.item import ItemRecord
from app.models.learner import Learner
from app.models.interaction import InteractionLog

router = APIRouter(tags=["analytics"])


@router.get("/scores")
async def get_scores(
    lab: str,
    session: AsyncSession = Depends(get_session)
) -> list[dict]:
    """Get score distribution for a lab in 4 buckets."""
    # Находим лабу (ищем "Lab 04" и "Lab 4")
    lab_num = lab.split('-')[-1]
    lab_num_padded = lab_num.zfill(2)
    lab_num_unpadded = lab_num.lstrip('0') or '0'
    lab_title_padded = f"Lab {lab_num_padded}"
    lab_title_unpadded = f"Lab {lab_num_unpadded}"
    
    lab_item = await session.execute(
        select(ItemRecord).where(
            ItemRecord.type == "lab",
            (ItemRecord.title.contains(lab_title_padded) | 
             ItemRecord.title.contains(lab_title_unpadded))
        )
    )
    lab_item = lab_item.scalar_one_or_none()
    if not lab_item:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0}
        ]
    
    # Находим все таски этой лабы
    tasks = await session.execute(
        select(ItemRecord.id).where(
            ItemRecord.type == "task",
            ItemRecord.parent_id == lab_item.id
        )
    )
    task_ids = [t[0] for t in tasks.all()]
    
    if not task_ids:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0}
        ]
    
    # Получаем все scores
    stmt = select(InteractionLog.score).where(
        InteractionLog.item_id.in_(task_ids),
        InteractionLog.score.isnot(None)
    )
    result = await session.execute(stmt)
    scores = [r[0] for r in result.all()]
    
    # Распределяем по бакетам
    buckets = {
        "0-25": 0,
        "26-50": 0,
        "51-75": 0,
        "76-100": 0
    }
    
    for score in scores:
        if score <= 25:
            buckets["0-25"] += 1
        elif score <= 50:
            buckets["26-50"] += 1
        elif score <= 75:
            buckets["51-75"] += 1
        else:
            buckets["76-100"] += 1
    
    return [
        {"bucket": k, "count": v}
        for k, v in buckets.items()
    ]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str,
    session: AsyncSession = Depends(get_session)
) -> list[dict]:
    """Get average score and attempt count per task for a lab."""
    # Находим лабу
    lab_num = lab.split('-')[-1]
    lab_num_padded = lab_num.zfill(2)
    lab_num_unpadded = lab_num.lstrip('0') or '0'
    lab_title_padded = f"Lab {lab_num_padded}"
    lab_title_unpadded = f"Lab {lab_num_unpadded}"
    
    lab_item = await session.execute(
        select(ItemRecord).where(
            ItemRecord.type == "lab",
            (ItemRecord.title.contains(lab_title_padded) | 
             ItemRecord.title.contains(lab_title_unpadded))
        )
    )
    lab_item = lab_item.scalar_one_or_none()
    if not lab_item:
        return []
    
    # Находим все таски с их взаимодействиями
    stmt = (
        select(
            ItemRecord.title,
            func.avg(InteractionLog.score).label("avg_score"),
            func.count(InteractionLog.id).label("attempts")
        )
        .join(InteractionLog, ItemRecord.id == InteractionLog.item_id)
        .where(
            ItemRecord.type == "task",
            ItemRecord.parent_id == lab_item.id,
            InteractionLog.score.isnot(None)
        )
        .group_by(ItemRecord.id, ItemRecord.title)
        .order_by(ItemRecord.title)
    )
    
    result = await session.execute(stmt)
    return [
        {
            "task": r.title,
            "avg_score": round(float(r.avg_score), 1),
            "attempts": r.attempts
        }
        for r in result.all()
    ]


@router.get("/timeline")
async def get_timeline(
    lab: str,
    session: AsyncSession = Depends(get_session)
) -> list[dict]:
    """Get submission count per day for a lab."""
    # Находим лабу
    lab_num = lab.split('-')[-1]
    lab_num_padded = lab_num.zfill(2)
    lab_num_unpadded = lab_num.lstrip('0') or '0'
    lab_title_padded = f"Lab {lab_num_padded}"
    lab_title_unpadded = f"Lab {lab_num_unpadded}"
    
    lab_item = await session.execute(
        select(ItemRecord).where(
            ItemRecord.type == "lab",
            (ItemRecord.title.contains(lab_title_padded) | 
             ItemRecord.title.contains(lab_title_unpadded))
        )
    )
    lab_item = lab_item.scalar_one_or_none()
    if not lab_item:
        return []
    
    # Для SQLite нужно использовать другой подход с датами
    from sqlalchemy import func
    
    # Получаем все взаимодействия для этой лабы
    stmt = (
        select(
            InteractionLog.created_at,
            InteractionLog.id
        )
        .join(ItemRecord, ItemRecord.id == InteractionLog.item_id)
        .where(
            ItemRecord.parent_id == lab_item.id,
            ItemRecord.type == "task"
        )
    )
    
    result = await session.execute(stmt)
    rows = result.all()
    
    # Группируем по дате в Python (более надежно для SQLite)
    from collections import defaultdict
    daily_counts = defaultdict(int)
    
    for row in rows:
        # Преобразуем created_at в строку даты
        if hasattr(row.created_at, 'strftime'):
            date_str = row.created_at.strftime("%Y-%m-%d")
        else:
            # Если это строка, пытаемся распарсить
            date_str = str(row.created_at).split()[0]
        daily_counts[date_str] += 1
    
    # Сортируем по дате
    result_list = [
        {"date": date, "submissions": count}
        for date, count in sorted(daily_counts.items())
    ]
    
    return result_list


@router.get("/groups")
async def get_groups(
    lab: str,
    session: AsyncSession = Depends(get_session)
) -> list[dict]:
    """Get average score and student count per group for a lab."""
    # Находим лабу
    lab_num = lab.split('-')[-1]
    lab_num_padded = lab_num.zfill(2)
    lab_num_unpadded = lab_num.lstrip('0') or '0'
    lab_title_padded = f"Lab {lab_num_padded}"
    lab_title_unpadded = f"Lab {lab_num_unpadded}"
    
    lab_item = await session.execute(
        select(ItemRecord).where(
            ItemRecord.type == "lab",
            (ItemRecord.title.contains(lab_title_padded) | 
             ItemRecord.title.contains(lab_title_unpadded))
        )
    )
    lab_item = lab_item.scalar_one_or_none()
    if not lab_item:
        return []
    
    # Группируем по группам
    stmt = (
        select(
            Learner.student_group.label("group"),
            func.avg(InteractionLog.score).label("avg_score"),
            func.count(func.distinct(Learner.id)).label("students")
        )
        .join(InteractionLog, Learner.id == InteractionLog.learner_id)
        .join(ItemRecord, ItemRecord.id == InteractionLog.item_id)
        .where(
            ItemRecord.parent_id == lab_item.id,
            ItemRecord.type == "task",
            InteractionLog.score.isnot(None),
            Learner.student_group.isnot(None)
        )
        .group_by(Learner.student_group)
        .order_by(Learner.student_group)
    )
    
    result = await session.execute(stmt)
    return [
        {
            "group": r.group,
            "avg_score": round(float(r.avg_score), 1),
            "students": r.students
        }
        for r in result.all()
    ]