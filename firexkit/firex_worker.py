import dataclasses
import enum
import uuid
from typing import ClassVar, Optional

from typing_extensions import Self


class FxWorkerTypes(enum.Enum):
    MC = 'mc'
    MASTER = 'master'
    WORKER = 'worker'

    @classmethod
    def fx_worker_type_from_str(
        cls,
        worker_name: str,
    ) -> Optional['FxWorkerTypes']:
        for t in cls:
            if worker_name.startswith(t.value):
                return t
        return None

    @classmethod
    def get_subworker_name(cls, worker_name: str) -> str:
        if worker_name is not None and (
            worker_type := cls.fx_worker_type_from_str(worker_name)
        ) in [FxWorkerTypes.MC, FxWorkerTypes.MASTER]:
            return worker_name.replace(
                worker_type.value,
                FxWorkerTypes.WORKER.value,
            )
        return worker_name


@dataclasses.dataclass
class FxWorkerName:
    queue_name: str
    spawn_group: str | None = None

    Queue : ClassVar[type[FxWorkerTypes]] = FxWorkerTypes

    def queue_and_sgroup(self) -> str:
        prefix = self.queue_name
        if self.spawn_group:
            prefix += f':{self.spawn_group}'
        return prefix

    def get_subworker_name(self) -> 'FxWorkerName':
        return FxWorkerName(
            FxWorkerTypes.get_subworker_name(self.queue_name),
            spawn_group=self.spawn_group,
        )

    def as_host_worker(self, host: str) -> 'FxWorkerHostName':
        return FxWorkerHostName(
            queue_name=self.queue_name,
            spawn_group=self.spawn_group,
            host=host,
        )

    def __str__(self):
        """
            e.g. master:g2 or master
        """
        return self.queue_and_sgroup()

    @classmethod
    def fx_worker_name_from_str(
        cls,
        worker_name: str,
    ) -> Self:
        parts = worker_name.split(':', maxsplit=2)
        if len(parts) > 1:
            spawn_group = ':'.join(parts[1:])
        else:
            spawn_group = None
        return cls(
            queue_name=parts[0],
            spawn_group=spawn_group,
        )

    @classmethod
    def fx_worker_name_from_queue_and_sg(
        cls,
        fx_queue: FxWorkerTypes,
        spawn_group: str | None,
    ) -> Self:
        return cls(
            queue_name=fx_queue.value,
            spawn_group=spawn_group,
        )


@dataclasses.dataclass
class FxWorkerHostName(FxWorkerName):
    host: str = ''

    def __post_init__(self):
        assert self.host, 'FxWorkerHostName must have host'

    def __str__(self):
        """
            e.g. master:g2@some-ad-hostname
        """
        worker_str = self.queue_name
        if self.spawn_group:
            worker_str += f':{self.spawn_group}'
        return f'{self.queue_and_sgroup()}@{self.host}'


@dataclasses.dataclass
class FxWorkerId(FxWorkerHostName):
    uniq_slug: str = dataclasses.field(
        default_factory=lambda: str(uuid.uuid4())[:8]
    )

    def __str__(self):
        """
            e.g. master:g2:@some-ad-hostname
        """
        worker_str = self.queue_name
        if self.spawn_group:
            worker_str += f':{self.spawn_group}'
        return f'{self.queue_and_sgroup()}:{self.uniq_slug}@{self.host}'
