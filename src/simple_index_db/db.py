import enum
import re
from functools import lru_cache
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

BUILD_TAG_REGEX = re.compile(r"^(?P<build_number>[0-9]+)(?P<build_string>.*)$")

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
    @lru_cache
    def from_str(cls, session, version_str: str) -> Self:
        version: Self = session.execute(
            select(Version).filter_by(version=version_str)
        ).scalar_one_or_none()
        if version is not None:
            return version
        try:
            parse_version(version_str)
            is_valid_vss = True
        except InvalidVersion:
            is_valid_vss = False
        version = cls(version=version_str, is_valid_vss=is_valid_vss)
        session.add(version)
        return version


class Hash(Base):
    __tablename__ = "hash"

    id: Mapped[int] = mapped_column(primary_key=True)
    file_id: Mapped[int] = mapped_column(ForeignKey("file.id"))
    file: Mapped["File"] = relationship(back_populates="hashes")
    algorithm: Mapped[str]
    hash_value: Mapped[str]

    @classmethod
    def from_info(cls, session, algorithm: str, hash_value: str) -> Self:
        hash_obj = cls(algorithm=algorithm, hash_value=hash_value)
        return hash_obj


class BuildTag(Base):
    __tablename__ = "build_tag"
    __table_args__ = (
        UniqueConstraint("tag"),
        Index("idx_build_tag", "tag"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    tag: Mapped[str]
    build_number: Mapped[int]
    build_string: Mapped[str | None]

    @classmethod
    @lru_cache
    def from_str(cls, session, build_tag_str: str) -> Self | None:
        if build_tag_str is None or build_tag_str == "":
            return None
        build_tag: Self = session.execute(
            select(BuildTag).filter_by(
                tag=build_tag_str,
            )
        ).scalar_one_or_none()
        if build_tag is not None:
            return build_tag
        m = BUILD_TAG_REGEX.match(build_tag_str)
        if m is None:
            raise ValueError(f"Invalid build tag string: {build_tag_str}")
        build_number = int(m.group("build_number"))
        build_string = m.group("build_string")
        build_tag = cls(
            tag=build_tag_str,
            build_number=build_number,
            build_string=build_string,
        )
        session.add(build_tag)
        return build_tag


class PythonTag(Base):
    __tablename__ = "python_tag"
    __table_args__ = (
        UniqueConstraint("tag"),
        Index("idx_python_tag", "tag"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    tag: Mapped[str]

    @classmethod
    @lru_cache
    def from_str(cls, session, tag_str: str) -> Self:
        python_tag: Self = session.execute(
            select(PythonTag).filter_by(tag=tag_str)
        ).scalar_one_or_none()
        if python_tag is not None:
            return python_tag
        python_tag = cls(tag=tag_str)
        session.add(python_tag)
        return python_tag


class AbiTag(Base):
    __tablename__ = "abi_tag"
    __table_args__ = (
        UniqueConstraint("tag"),
        Index("idx_abi_tag", "tag"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    tag: Mapped[str]

    @classmethod
    @lru_cache
    def from_str(cls, session, tag_str: str) -> Self:
        abi_tag: Self = session.execute(
            select(AbiTag).filter_by(tag=tag_str)
        ).scalar_one_or_none()
        if abi_tag is not None:
            return abi_tag
        abi_tag = cls(tag=tag_str)
        session.add(abi_tag)
        return abi_tag


class PlatformTag(Base):
    __tablename__ = "platform_tag"
    __table_args__ = (
        UniqueConstraint("tag"),
        Index("idx_platform_tag", "tag"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    tag: Mapped[str]

    @classmethod
    @lru_cache
    def from_str(cls, session, tag_str: str) -> Self:
        platform_tag: Self = session.execute(
            select(PlatformTag).filter_by(tag=tag_str)
        ).scalar_one_or_none()
        if platform_tag is not None:
            return platform_tag
        platform_tag = cls(tag=tag_str)
        session.add(platform_tag)
        return platform_tag


class Wheel(Base):
    __tablename__ = "wheel"
    __table_args__ = (
        UniqueConstraint("file_id"),
        Index("idx_wheel_file_id", "file_id"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    file_id: Mapped[int] = mapped_column(ForeignKey("file.id"))
    file: Mapped["File"] = relationship(back_populates="wheel")
    version_id: Mapped[int] = mapped_column(ForeignKey("version.id"))
    version: Mapped[Version] = relationship()
    build_tag_id: Mapped[int | None] = mapped_column(ForeignKey("build_tag.id"))
    build_tag: Mapped[BuildTag | None] = relationship()
    python_tag_id: Mapped[int] = mapped_column(ForeignKey("python_tag.id"))
    python_tag: Mapped[PythonTag] = relationship()
    abi_tag_id: Mapped[int] = mapped_column(ForeignKey("abi_tag.id"))
    abi_tag: Mapped[AbiTag] = relationship()
    platform_tag_id: Mapped[int] = mapped_column(ForeignKey("platform_tag.id"))
    platform_tag: Mapped[PlatformTag] = relationship()

    @classmethod
    def from_file(cls, session, file: "File") -> Self | None:
        filename = file.filename
        assert filename.endswith(".whl")
        wheel_name = filename[:-4]
        parts = wheel_name.split("-")
        if len(parts) < 5 or len(parts) > 6:
            return None
        if len(parts) == 5:
            (
                name,
                version_str,
                python_tag_str,
                abi_tag_str,
                platform_tag_str,
            ) = parts
            build_tag = None
        else:
            (
                name,
                version_str,
                build_tag_str,
                python_tag_str,
                abi_tag_str,
                platform_tag_str,
            ) = parts
            try:
                build_tag = BuildTag.from_str(session, build_tag_str)
            except ValueError:
                return None
        version = Version.from_str(session, version_str)
        return cls(
            version=version,
            build_tag=build_tag,
            python_tag=PythonTag.from_str(session, python_tag_str),
            abi_tag=AbiTag.from_str(session, abi_tag_str),
            platform_tag=PlatformTag.from_str(session, platform_tag_str),
        )


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
    hashes: Mapped[set[Hash]] = relationship(
        back_populates="file", cascade="all, delete-orphan"
    )
    requires_python: Mapped[str | None]
    core_metadata: Mapped[str | None]
    gpg_signature: Mapped[str | None]
    yanked: Mapped[bool]
    yanked_reason: Mapped[str | None]
    size: Mapped[int]
    upload_time: Mapped[str | None]
    provenance: Mapped[str | None]
    wheel: Mapped[Wheel | None] = relationship(
        back_populates="file", cascade="all, delete-orphan"
    )

    @classmethod
    def from_info(cls, session, file_info: dict) -> Self:
        filename = file_info["filename"]
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
        file = cls(
            filename=filename,
            url=file_info["url"],
            hashes=hashes,
            requires_python=file_info.get("requires-python", None),
            core_metadata=None,  # file_info.get("core-metadata", None),
            gpg_signature=None,  # file_info.get("gpg-signature", None),
            yanked=yanked,
            yanked_reason=yanked_reason,
            size=file_info.get("size", 0),
            upload_time=file_info.get("upload-time", None),
            provenance=file_info.get("provenance", None),
            wheel=None,
        )
        if filename.endswith(".whl"):
            file.wheel = Wheel.from_file(session, file)
        return file


class Project(Base):
    __tablename__ = "project"
    __table_args__ = (
        UniqueConstraint("name"),
        Index("idx_name", "name"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]
    files: Mapped[set[File]] = relationship(
        back_populates="project", cascade="all, delete-orphan"
    )
    versions: Mapped[set[Version]] = relationship(secondary=project_version_association)
    last_serial: Mapped[int]
    status: Mapped[ProjectStatus | None]
    status_reason: Mapped[str | None]

    @classmethod
    def from_info(cls, session, project_last_serial, project_info) -> Self:
        status = project_info.get("project-status", {}).get("status", None)
        if status is not None:
            status = ProjectStatus(status)
        versions = {
            Version.from_str(session, v) for v in project_info.get("versions", [])
        }
        files = {File.from_info(session, f) for f in project_info.get("files", [])}
        return cls(
            name=project_info["name"],
            last_serial=project_last_serial,
            status=status,
            status_reason=project_info.get("project-status-reason", None),
            versions=versions,
            files=files,
        )

    def update_from_info(self, session, project_last_serial, project_info) -> None:
        status = project_info.get("project-status", {}).get("status", None)
        if status is not None:
            status = ProjectStatus(status)
        self.status = status
        self.status_reason = project_info.get("project-status-reason", None)
        old_versions = {v.version for v in self.versions}
        new_versions = {
            Version.from_str(session, v)
            for v in project_info.get("versions", [])
            if v not in old_versions
        }
        self.versions |= new_versions
        old_files = {f.filename for f in self.files}
        new_files = {
            File.from_info(session, f)
            for f in project_info.get("files", [])
            if f["filename"] not in old_files
        }
        self.files |= new_files
        self.last_serial = project_last_serial


def init_db():
    global engine
    if engine is None:
        engine = create_engine(
            "sqlite:///simple_index_db.sqlite3",
            connect_args={"autocommit": False},
        )
    Base.metadata.create_all(engine)
    return sessionmaker(engine)
