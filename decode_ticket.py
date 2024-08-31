# Based on JavaScript from https://ticket.iroh.computer/
import base64
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional

class TicketType(Enum):
    NODE = "node"
    BLOB = "blob"
    DOC = "doc"

class BlobFormat(Enum):
    HASH_SEQ = "HashSeq"
    RAW = "Raw"

class Capability(Enum):
    READ = "Read"
    WRITE = "Write"

@dataclass
class AddrInfo:
    derp_url: Optional[str]
    direct_addresses: List[str]

@dataclass
class NodeAddr:
    node_id: str
    info: AddrInfo

@dataclass
class NodeTicket:
    type: TicketType
    node: NodeAddr

@dataclass
class BlobTicket:
    type: TicketType
    node: NodeAddr
    format: BlobFormat
    hash: str

@dataclass
class DocTicket:
    type: TicketType
    capability: Capability
    namespace: str
    nodes: List[NodeAddr]

class TicketDecoder:
    def __init__(self, ticket_string: str):
        self.offset = 0
        if ticket_string.startswith("node"):
            ticket_string = ticket_string[4:]
            self.type = TicketType.NODE
        elif ticket_string.startswith("blob"):
            ticket_string = ticket_string[4:]
            self.type = TicketType.BLOB
        elif ticket_string.startswith("doc"):
            ticket_string = ticket_string[3:]
            self.type = TicketType.DOC
        else:
            raise ValueError("Unknown ticket type")

        # Add padding if necessary
        padding = '=' * ((8 - len(ticket_string) % 8) % 8)
        padded_ticket = ticket_string.upper() + padding
        
        self.buffer = base64.b32decode(padded_ticket)

    def decode(self):
        if self.type == TicketType.NODE:
            return self.read_node_ticket()
        elif self.type == TicketType.BLOB:
            return self.read_blob_ticket()
        elif self.type == TicketType.DOC:
            return self.read_document_ticket()

    def read_node_ticket(self) -> NodeTicket:
        if self.read_u8() != 0:
            raise ValueError("Expected variant 0")
        return NodeTicket(
            type=TicketType.NODE,
            node=self.read_node_addr()
        )

    def read_blob_ticket(self) -> BlobTicket:
        if self.read_u8() != 0:
            raise ValueError("Expected variant 0")
        return BlobTicket(
            type=TicketType.BLOB,
            node=self.read_node_addr(),
            format=self.read_blob_format(),
            hash=self.read_hash()
        )

    def read_document_ticket(self) -> DocTicket:
        if self.read_u8() != 0:
            raise ValueError("Expected variant 0")
        capability = self.read_capability()
        namespace = ""
        if capability == Capability.READ:
            namespace = self.read_hash()
        elif capability == Capability.WRITE:
            namespace = self.read_secret_key()
        else:
            raise ValueError(f"Unknown capability: {capability}")
        
        return DocTicket(
            type=TicketType.DOC,
            capability=capability,
            namespace=namespace,
            nodes=self.read_node_addrs()
        )

    def read_capability(self) -> Capability:
        value = self.read_varint()
        if value == 0:
            return Capability.WRITE
        elif value == 1:
            return Capability.READ
        else:
            raise ValueError(f"Unknown capability: {value}")

    def read_blob_format(self) -> BlobFormat:
        value = self.read_varint()
        if value == 0:
            return BlobFormat.RAW
        elif value == 1:
            return BlobFormat.HASH_SEQ
        else:
            raise ValueError(f"Unknown blob format: {value}")

    def read_node_addrs(self) -> List[NodeAddr]:
        count = self.read_varint()
        return [self.read_node_addr() for _ in range(count)]

    def read_node_addr(self) -> NodeAddr:
        node_id = self.read_node_id()
        info = self.read_addr_info()
        return NodeAddr(node_id=node_id, info=info)

    def read_node_id(self) -> str:
        node_id = self.buffer[self.offset:self.offset + 32]
        self.offset += 32
        return base64.b32encode(node_id).decode().lower().rstrip('=')

    def read_addr_info(self) -> AddrInfo:
        derp_url = None
        if self.read_option():
            derp_url = self.read_string()
        return AddrInfo(
            derp_url=derp_url,
            direct_addresses=self.read_addresses()
        )

    def read_addresses(self) -> List[str]:
        count = self.read_varint()
        return [self.read_socket_addr() for _ in range(count)]

    def read_socket_addr(self) -> str:
        version = self.read_varint()
        if version == 0:
            return self.read_ipv4()
        elif version == 1:
            return self.read_ipv6()
        else:
            raise ValueError(f"Unknown IP version: {version}")

    def read_ipv4(self) -> str:
        ip = '.'.join(str(self.buffer[self.offset + i]) for i in range(4))
        self.offset += 4
        port = self.read_varint()
        return f"{ip}:{port}"

    def read_ipv6(self) -> str:
        ip = ':'.join(f"{self.buffer[self.offset + i*2]:02x}{self.buffer[self.offset + i*2 + 1]:02x}" for i in range(8))
        self.offset += 16
        port = self.read_varint()
        return f"[{ip}]:{port}"

    def read_secret_key(self) -> str:
        key = self.buffer[self.offset:self.offset + 33]
        self.offset += 33
        return base64.b32encode(key).decode().lower().rstrip('=')

    def read_hash(self) -> str:
        hash_value = self.buffer[self.offset:self.offset + 32]
        self.offset += 32
        return base64.b32encode(hash_value).decode().lower().rstrip('=')

    def read_option(self) -> bool:
        return self.read_u8() == 1

    def read_string(self) -> str:
        length = self.read_varint()
        string = self.buffer[self.offset:self.offset + length].decode('utf-8')
        self.offset += length
        return string

    def read_u32(self) -> int:
        return self.read_varint()

    def read_u8(self) -> int:
        value = self.buffer[self.offset]
        self.offset += 1
        return value

    def read_varint(self) -> int:
        value = 0
        shift = 0
        while True:
            byte = self.buffer[self.offset]
            self.offset += 1
            value |= (byte & 0x7F) << shift
            if (byte & 0x80) == 0:
                break
            shift += 7
        return value

def decode_iroh_ticket(ticket_string: str):
    decoder = TicketDecoder(ticket_string)
    return decoder.decode()

# # Example usage
# ticket = "this-is-not-a-real-ticket"
# decoded_ticket = decode_iroh_ticket(ticket)
# print(decoded_ticket)

# # To get the node ID and relay from a DocTicket
# if isinstance(decoded_ticket, DocTicket) and decoded_ticket.nodes:
#     node = decoded_ticket.nodes[0]
#     print(f"Node ID: {node.node_id}")
#     print(f"Relay URL: {node.info.derp_url}")