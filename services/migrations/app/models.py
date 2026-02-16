from __future__ import annotations

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class BootstrapHeartbeat(Base):
    __tablename__ = "bootstrap_heartbeat"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)


class Submitter(Base):
    __tablename__ = "submitter"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    submitter_id: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    display_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)


class FileType(Base):
    __tablename__ = "file_type"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)


class LayoutRegistry(Base):
    __tablename__ = "layout_registry"
    __table_args__ = (UniqueConstraint("file_type", "layout_version", name="uq_layout_registry_type_version"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    file_type: Mapped[str] = mapped_column(Text, nullable=False)
    layout_version: Mapped[str] = mapped_column(Text, nullable=False)
    schema_json: Mapped[str] = mapped_column(Text, nullable=False)
    parser_config_json: Mapped[str] = mapped_column(Text, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)


class InboxObject(Base):
    __tablename__ = "inbox_object"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    object_key: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    grouping_key: Mapped[str] = mapped_column(Text, nullable=False)
    size_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    etag: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_modified: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    first_seen_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)


class Submission(Base):
    __tablename__ = "submission"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    submission_id: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    submitter_id: Mapped[str] = mapped_column(Text, ForeignKey("submitter.submitter_id"), nullable=False)
    grouping_key: Mapped[str] = mapped_column(Text, nullable=False)
    fingerprint: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    state: Mapped[str] = mapped_column(Text, nullable=False)
    inferred_file_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    layout_version: Mapped[str | None] = mapped_column(Text, nullable=True)
    coverage_start_month: Mapped[str | None] = mapped_column(Text, nullable=True)
    coverage_end_month: Mapped[str | None] = mapped_column(Text, nullable=True)
    grouping_method: Mapped[str] = mapped_column(Text, nullable=False)
    group_window_start: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    group_window_end: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    raw_prefix: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)


class SubmissionFile(Base):
    __tablename__ = "submission_file"
    __table_args__ = (UniqueConstraint("submission_id", "object_key", name="uq_submission_file_key"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    submission_id: Mapped[str] = mapped_column(Text, ForeignKey("submission.submission_id"), nullable=False)
    object_key: Mapped[str] = mapped_column(Text, nullable=False)
    raw_object_key: Mapped[str] = mapped_column(Text, nullable=False)
    size_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    etag: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_modified: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
