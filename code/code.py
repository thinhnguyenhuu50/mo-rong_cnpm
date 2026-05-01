import datetime
import logging
import os
from dataclasses import dataclass

from celery import Celery
from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, Text, create_engine
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

# Architecture initialization: message broker and database configuration
DATABASE_URI = os.getenv("PROD_DATABASE_URL", "sqlite:///sers_local.db")
REDIS_BROKER = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")

app_celery = Celery("sers_async_worker", broker=REDIS_BROKER)
app_celery.conf.timezone = "UTC"

engine = create_engine(DATABASE_URI)
Base = declarative_base()
Session = sessionmaker(bind=engine)

logger = logging.getLogger("sers.notification")
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


class ReminderStatus:
    PENDING = "Pending"
    DISPATCHED = "Dispatched"
    FAILED = "Failed"
    CANCELLED = "Cancelled"


@dataclass
class EventPayload:
    title: str
    description: str
    location: str
    event_timestamp: datetime.datetime
    target_email: str
    offset_minutes: int
    user_id: int

    def validate(self, current_time):
        if not self.title.strip() or not self.target_email.strip():
            raise ValueError("Title and target_email are mandatory fields.")
        if self.offset_minutes < 0:
            raise ValueError("offset_minutes must be a non-negative integer.")
        if self.event_timestamp <= current_time:
            raise ValueError("Scheduled events must occur in the future.")


# Model layer: entities mapped from the UML class design
class UserEntity(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    username = Column(String(120), nullable=False)
    email = Column(String(150), nullable=False, unique=True)
    password_hash = Column(String(255), nullable=False)

    events = relationship("EventEntity", back_populates="user", cascade="all, delete-orphan")


class EventEntity(Base):
    __tablename__ = "scheduled_events"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    title = Column(String(150), nullable=False)
    description = Column(Text, nullable=False, default="")
    location = Column(String(150), nullable=False, default="")
    event_timestamp = Column(DateTime, nullable=False)
    target_email = Column(String(150), nullable=False)

    user = relationship("UserEntity", back_populates="events")
    reminders = relationship("ReminderEntity", back_populates="event", cascade="all, delete-orphan")


class ReminderEntity(Base):
    __tablename__ = "reminders"

    id = Column(Integer, primary_key=True)
    event_id = Column(Integer, ForeignKey("scheduled_events.id"), nullable=False)
    trigger_offset = Column(Integer, nullable=False)
    status = Column(String(20), nullable=False, default=ReminderStatus.PENDING)
    celery_task_id = Column(String(100), nullable=True)

    event = relationship("EventEntity", back_populates="reminders")


Base.metadata.create_all(engine)


# Asynchronous worker layer: runs independently from the web server
@app_celery.task(name="notification.dispatch", bind=True)
def dispatch_reminder_alert(self, reminder_id, target_email, event_title):
    """Background worker that simulates sending reminder notifications."""
    try:
        alert_payload = "Automated Reminder: '{}' is approaching.".format(event_title)
        logger.info("SUCCESS reminder_id=%s target=%s payload=%s", reminder_id, target_email, alert_payload)
        return {"execution_status": "success", "reminder_id": reminder_id}
    except Exception as runtime_error:
        logger.exception("FAILED reminder_id=%s target=%s", reminder_id, target_email)
        return {"execution_status": "failed", "error_trace": str(runtime_error)}


# Controller layer: orchestrates model persistence and broker dispatch
class SersController:
    def __init__(self):
        self.db_session = Session()

    def _calculate_trigger_time(self, event_timestamp, offset_minutes, current_time):
        calculated_trigger = event_timestamp - datetime.timedelta(minutes=offset_minutes)
        if calculated_trigger < current_time:
            return current_time + datetime.timedelta(seconds=5)
        return calculated_trigger

    def _dispatch_to_broker(self, reminder_id, target_email, event_title, trigger_time):
        return dispatch_reminder_alert.apply_async(
            args=[reminder_id, target_email, event_title],
            eta=trigger_time,
        )

    def schedule_new_event(self, payload):
        """Persist event data and schedule a Celery task for deferred delivery."""
        current_system_time = datetime.datetime.utcnow()
        payload.validate(current_system_time)

        new_event = EventEntity(
            user_id=payload.user_id,
            title=payload.title,
            description=payload.description,
            location=payload.location,
            event_timestamp=payload.event_timestamp,
            target_email=payload.target_email,
        )
        self.db_session.add(new_event)
        self.db_session.flush()

        reminder = ReminderEntity(
            event_id=new_event.id,
            trigger_offset=payload.offset_minutes,
            status=ReminderStatus.PENDING,
        )
        self.db_session.add(reminder)
        self.db_session.flush()

        trigger_time = self._calculate_trigger_time(
            event_timestamp=payload.event_timestamp,
            offset_minutes=payload.offset_minutes,
            current_time=current_system_time,
        )
        dispatched_task = self._dispatch_to_broker(
            reminder_id=reminder.id,
            target_email=payload.target_email,
            event_title=payload.title,
            trigger_time=trigger_time,
        )

        reminder.celery_task_id = dispatched_task.id
        self.db_session.commit()
        return new_event.id

    def revoke_existing_event(self, entity_id):
        """Delete an event and revoke its scheduled asynchronous reminder task."""
        target_event = self.db_session.query(EventEntity).filter_by(id=entity_id).first()
        if not target_event:
            return False

        for reminder in target_event.reminders:
            if reminder.celery_task_id:
                app_celery.control.revoke(reminder.celery_task_id, terminate=True)
            reminder.status = ReminderStatus.CANCELLED

        self.db_session.delete(target_event)
        self.db_session.commit()
        return True

    def close(self):
        self.db_session.close()