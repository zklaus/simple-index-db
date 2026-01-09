import enum
import re
from threading import Lock
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
    event,
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


class TagCache:
    """Thread-safe cache for tracking known tag strings to avoid redundant existence checks."""

    def __init__(self):
        self._known_tags = dict()
        self._lock = Lock()
        self._loaded = False

    def contains(self, tag_str: str) -> bool:
        """Check if tag string is known to exist."""
        return tag_str in self._known_tags

    def get_id(self, tag_str: str) -> int | None:
        """Get the ID of a known tag string, or None if not known."""
        return self._known_tags.get(tag_str, None)

    def add(self, tag_str: str, id: int):
        """Mark a tag string as known to exist."""
        with self._lock:
            self._known_tags[tag_str] = id

    def load_from_query(self, tag_strings: list[tuple[str, int]]):
        """Bulk load known tag strings."""
        with self._lock:
            self._known_tags.update(tag_strings)
            self._loaded = True

    def is_loaded(self) -> bool:
        """Check if cache has been pre-loaded."""
        return self._loaded

    def size(self) -> int:
        """Return the number of cached tags."""
        return len(self._known_tags)


# Global caches for each tag type
_version_cache = TagCache()
_build_tag_cache = TagCache()
_python_tag_cache = TagCache()
_abi_tag_cache = TagCache()
_platform_tag_cache = TagCache()


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
        # Check if we know this version exists (avoids query)
        if (version_id := _version_cache.get_id(version_str)):
            version: Self = session.get_one(Version, version_id)
            return version

        # Might not exist, check database
        version: Self = session.execute(
            select(Version).filter_by(version=version_str)
        ).scalar_one_or_none()

        if version is not None:
            _version_cache.add(version_str, version.id)
            return version

        # Create new version
        try:
            parse_version(version_str)
            is_valid_vss = True
        except InvalidVersion:
            is_valid_vss = False

        version = cls(version=version_str, is_valid_vss=is_valid_vss)
        session.add(version)
        _version_cache.add(version_str, version.id)
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
    def from_str(cls, session, build_tag_str: str) -> Self | None:
        if build_tag_str is None or build_tag_str == "":
            return None

        # Check if we know this build tag exists (avoids query)
        if (tag_id := _build_tag_cache.get_id(build_tag_str)):
            build_tag: Self = session.get_one(BuildTag, tag_id)
            return build_tag

        # Might not exist, check database
        build_tag: Self = session.execute(
            select(BuildTag).filter_by(tag=build_tag_str)
        ).scalar_one_or_none()

        if build_tag is not None:
            _build_tag_cache.add(build_tag_str, build_tag.id)
            return build_tag

        # Create new build tag
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
        _build_tag_cache.add(build_tag_str, build_tag.id)
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
    def from_str(cls, session, tag_str: str) -> Self:
        # Check if we know this python tag exists (avoids query)
        if (tag_id := _python_tag_cache.get_id(tag_str)):
            python_tag: Self = session.get_one(PythonTag, tag_id)
            return python_tag

        # Might not exist, check database
        python_tag: Self = session.execute(
            select(PythonTag).filter_by(tag=tag_str)
        ).scalar_one_or_none()

        if python_tag is not None:
            _python_tag_cache.add(tag_str, python_tag.id)
            return python_tag

        # Create new python tag
        python_tag = cls(tag=tag_str)
        session.add(python_tag)
        _python_tag_cache.add(tag_str, python_tag.id)
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
    def from_str(cls, session, tag_str: str) -> Self:
        # Check if we know this abi tag exists (avoids query)
        if (tag_id := _abi_tag_cache.get_id(tag_str)):
            abi_tag: Self = session.get_one(AbiTag, tag_id)
            return abi_tag

        # Might not exist, check database
        abi_tag: Self = session.execute(
            select(AbiTag).filter_by(tag=tag_str)
        ).scalar_one_or_none()

        if abi_tag is not None:
            _abi_tag_cache.add(tag_str, abi_tag.id)
            return abi_tag

        # Create new abi tag
        abi_tag = cls(tag=tag_str)
        session.add(abi_tag)
        _abi_tag_cache.add(tag_str, abi_tag.id)
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
    def from_str(cls, session, tag_str: str) -> Self:
        # Check if we know this platform tag exists (avoids query)
        if (tag_id := _platform_tag_cache.get_id(tag_str)):
            platform_tag: Self = session.get_one(PlatformTag, tag_id)
            return platform_tag

        # Might not exist, check database
        platform_tag: Self = session.execute(
            select(PlatformTag).filter_by(tag=tag_str)
        ).scalar_one_or_none()

        if platform_tag is not None:
            _platform_tag_cache.add(tag_str, platform_tag.id)
            return platform_tag

        # Create new platform tag
        platform_tag = cls(tag=tag_str)
        session.add(platform_tag)
        _platform_tag_cache.add(tag_str, platform_tag.id)
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


def _set_sqlite_pragma(dbapi_conn, connection_record):
    """Set SQLite pragmas for optimal performance."""
    cursor = dbapi_conn.cursor()
    # Enable Write-Ahead Logging for better concurrency
    cursor.execute("PRAGMA journal_mode=WAL")
    # NORMAL synchronous mode is safe with WAL and much faster
    cursor.execute("PRAGMA synchronous=NORMAL")
    # 64MB cache size for better performance
    cursor.execute("PRAGMA cache_size=-64000")
    # Store temporary tables in memory
    cursor.execute("PRAGMA temp_store=MEMORY")
    # Increase page size for better I/O (must be set before any tables exist)
    cursor.execute("PRAGMA page_size=4096")
    cursor.close()


def load_tag_caches(session):
    """Pre-load all existing tag strings into memory caches for fast lookups."""
    if _version_cache.is_loaded():
        return  # Already loaded

    print("Loading tag caches...", end=" ", flush=True)

    # Load all version strings
    _version_cache.load_from_query(session.execute(select(Version.version, Version.id)).all())

    # Load all build tag strings
    _build_tag_cache.load_from_query(session.execute(select(BuildTag.tag, BuildTag.id)).all())

    # Load all python tag strings
    _python_tag_cache.load_from_query(session.execute(select(PythonTag.tag, PythonTag.id)).all())

    # Load all abi tag strings
    _abi_tag_cache.load_from_query(session.execute(select(AbiTag.tag, AbiTag.id)).all())

    # Load all platform tag strings
    _platform_tag_cache.load_from_query(session.execute(select(PlatformTag.tag, PlatformTag.id)).all())

    print(f"Loaded {_version_cache.size()} versions, "
          f"{_build_tag_cache.size()} build tags, "
          f"{_python_tag_cache.size()} python tags, "
          f"{_abi_tag_cache.size()} abi tags, "
          f"{_platform_tag_cache.size()} platform tags")


def init_db():
    global engine
    if engine is None:
        engine = create_engine(
            "sqlite:///simple_index_db.sqlite3",
            connect_args={
                "timeout": 30,
                "check_same_thread": False,
            },
            pool_size=10,
            max_overflow=20,
        )
        # Set SQLite pragmas on each connection
        event.listen(engine, "connect", _set_sqlite_pragma)
    Base.metadata.create_all(engine)
    Session = sessionmaker(engine)

    # Pre-load tag caches for performance
    with Session() as session:
        load_tag_caches(session)

    return Session
