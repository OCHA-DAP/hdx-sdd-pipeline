from typing import Dict, Any, Protocol


class IISPStrategy(Protocol):
    """
    Protocol defining the interface for ISP retrieval strategies.
    """

    def get_isps(self) -> Dict[str, Any]:
        """
        Retrieves ISP data and transforms it into a standard dictionary format.
        """
        ...
