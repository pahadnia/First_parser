from dataclasses import dataclass


@dataclass(frozen=True)
class Notebook:
    title: str
    url: str
    price: float
    description: str
    image: str = ''
    manufacturer: str = ''
    diagonal: str = ''
    screen_resolution: str = ''
    os: str = ''
    processor: str = ''
    op_mem: str = ''
    type_video_card: str = ''
    video_card: str = ''
    type_drive: str = ''
    capacity_drive: str = ''
    auto_word_time: str = ''
    state: str = ''

