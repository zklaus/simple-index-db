import enum
from typing import Self

from packaging.version import InvalidVersion
from packaging.version import parse as parse_version
from sqlalchemy import (
    Column,
    ForeignKey,
    Index,
    Table,
    UniqueConstraint,
    create_engine,
    select,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship,
    sessionmaker,
)

engine = None


class ProjectStatus(enum.Enum):
    ACTIVE = "active"
    ARCHIVED = "archived"
    QUARANTINED = "quarantined"
    DEPRECATED = "deprecated"


class Base(DeclarativeBase):
    pass


class Repository(Base):
    __tablename__ = "repository"

    id: Mapped[int] = mapped_column(primary_key=True)
    url: Mapped[str]
    last_fetched_serial: Mapped[int | None]


project_version_association = Table(
    "project_version_association",
    Base.metadata,
    Column("project_id", ForeignKey("project.id")),
    Column("version_id", ForeignKey("version.id")),
)


class Version(Base):
    __tablename__ = "version"
    __table_args__ = (
        UniqueConstraint("version"),
        Index("idx_version", "version"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    version: Mapped[str]
    is_valid_vss: Mapped[bool]

    @classmethod
    def from_str(cls, session, version_str: str) -> Self:
        version: Self = session.execute(select(Version).filter_by(version=version_str)).scalar_one_or_none()
        if version is not None:
            return version
        try:
            parse_version(version_str)
            is_valid_vss = True
        except InvalidVersion:
            is_valid_vss = False
        return cls(version=version_str, is_valid_vss=is_valid_vss)
    

class Hash(Base):
    __tablename__ = "hash"
    __table_args__ = (
        UniqueConstraint("algorithm", "hash_value"),
        Index("idx_alg_hash", "algorithm", "hash_value"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    file_id: Mapped[int] = mapped_column(ForeignKey("file.id"))
    file: Mapped["File"] = relationship(back_populates="hashes")
    algorithm: Mapped[str]
    hash_value: Mapped[str]

    @classmethod
    def from_info(cls, session, algorithm: str, hash_value: str) -> Self:
        hash_obj: Self = session.execute(
            select(Hash).filter_by(algorithm=algorithm, hash_value=hash_value)
        ).scalar_one_or_none()
        if hash_obj is not None:
            return hash_obj
        return cls(algorithm=algorithm, hash_value=hash_value)


class File(Base):
    __tablename__ = "file"
    __table_args__ = (
        UniqueConstraint("filename"),
        Index("idx_filename", "filename"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    project_id: Mapped[int] = mapped_column(ForeignKey("project.id"))
    project: Mapped["Project"] = relationship(back_populates="files")
    filename: Mapped[str]
    url: Mapped[str]
    hashes: Mapped[set[Hash]] = relationship(back_populates="file")
    requires_python: Mapped[str | None]
    core_metadata: Mapped[str | None]
    gpg_signature: Mapped[str | None]
    yanked: Mapped[bool]
    yanked_reason: Mapped[str | None]
    size: Mapped[int]
    upload_time: Mapped[str | None]
    provenance: Mapped[str | None]

    @classmethod
    def from_info(cls, session, file_info: dict) -> Self:
        hashes = {
            Hash.from_info(session, algorithm, hash_value)
            for algorithm, hash_value in file_info.get("hashes", {}).items()
        }
        yanked_value = file_info.get("yanked", False)
        if yanked_value:
            yanked_reason = yanked_value
            yanked = True
        else:
            yanked_reason = None
            yanked = False
        return cls(
            filename=file_info["filename"],
            url=file_info["url"],
            hashes=hashes,
            requires_python=file_info.get("requires-python", None),
            core_metadata=None, #file_info.get("core-metadata", None),
            gpg_signature=None, #file_info.get("gpg-signature", None),
            yanked=yanked,
            yanked_reason=yanked_reason,
            size=file_info.get("size", 0),
            upload_time=file_info.get("upload-time", None),
            provenance=file_info.get("provenance", None),
        )


class Project(Base):
    __tablename__ = "project"
    __table_args__ = (
        UniqueConstraint("name"),
        Index("idx_name", "name"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]
    files: Mapped[set[File]] = relationship(back_populates="project")
    versions: Mapped[set[Version]] = relationship(secondary=project_version_association)
    last_serial: Mapped[int]
    status: Mapped[ProjectStatus | None]
    status_reason: Mapped[str | None]

    @classmethod
    def from_info(cls, session, project_last_serial, project_info) -> Self:
        status = project_info.get("project-status", {}).get("status", None)
        if status is not None:
            status = ProjectStatus(status)
        return cls(
            name=project_info["name"],
            last_serial=project_last_serial,
            status=status,
            status_reason=project_info.get("project-status-reason", None),
            versions = {Version.from_str(session, v) for v in project_info.get("versions", [])},
            files = {File.from_info(session, f) for f in project_info.get("files", [])},
        )
    
    def update_from_info(self, session, project_last_serial, project_info) -> None:
        self.last_serial = project_last_serial
        status = project_info.get("project-status", {}).get("status", None)
        if status is not None:
            status = ProjectStatus(status)
        self.status = status
        self.status_reason = project_info.get("project-status-reason", None)
        self.versions = {Version.from_str(session, v) for v in project_info.get("versions", [])}
        self.files = {File.from_info(session, f) for f in project_info.get("files", [])},


def init_db():
    global engine
    if engine is None:
        engine = create_engine("sqlite:///simple_index_db.sqlite3")
    Base.metadata.create_all(engine)
    return sessionmaker(engine)