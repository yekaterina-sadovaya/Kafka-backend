from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field, field_validator
import uuid


class OrderMessage(BaseModel):
    
    order_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Unique order identifier"
    )
    user: str = Field(
        min_length=1,
        max_length=100,
        description="Username"
    )
    item: str = Field(
        min_length=1,
        max_length=200,
        description="Item name"
    )
    quantity: int = Field(
        gt=0,
        le=10000,
        description="Quantity ordered"
    )
    price: Optional[float] = Field(
        default=None,
        ge=0,
        description="Price per item"
    )
    timestamp: datetime = Field(
        default_factory=datetime.utcnow,
        description="Order timestamp"
    )
    priority: str = Field(
        default="normal",
        description="Order priority"
    )
    
    @field_validator('priority')
    @classmethod
    def validate_priority(cls, v: str) -> str:
        valid_priorities = ['low', 'normal', 'high', 'urgent']
        if v.lower() not in valid_priorities:
            raise ValueError(f'priority must be one of {valid_priorities}')
        return v.lower()
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
    
    def get_partition_key(self) -> str:
        return self.user
    
    def to_json(self) -> str:
        return self.model_dump_json()
    
    @classmethod
    def from_json(cls, json_str: str) -> 'OrderMessage':
        return cls.model_validate_json(json_str)


class MessageMetadata(BaseModel):
    
    message_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    source: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    retry_count: int = 0
    correlation_id: Optional[str] = None
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
