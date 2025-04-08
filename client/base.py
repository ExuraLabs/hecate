from typing import Any, ClassVar, Generic, TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from client import HecateClient


T = TypeVar("T")  # Return type variable for methods


class AsyncOgmiosMethod(Generic[T]):
    """Base class for all async Ogmios methods.

    This base class uses composition with a declaratively specified
    Ogmios class via the ogmios_class class attribute.

    :param client: The client to use for the request.
    :type client: HecateClient
    """

    # Subclasses should override this with their specific Ogmios class
    ogmios_class: ClassVar[type] = None  # type: ignore
    parser_method_name: ClassVar[str] = ""

    def __init__(self, client: "HecateClient"):
        if self.ogmios_class is None:
            raise TypeError(f"{self.__class__.__name__} must define ogmios_class")

        self.client = client
        # Extract the method name from the ogmios class
        # We need an instance to access instance attributes
        temp_instance = self.ogmios_class(client)
        self.method = temp_instance.method

    async def execute(self, request_id: Any = None, **kwargs: Any) -> T:
        """Send and receive the request asynchronously.

        :param request_id: The ID of the request.
        :param kwargs: Additional parameters for the request.
        :return: The parsed response.
        """
        await self.send(request_id, **kwargs)
        return await self.receive()

    async def send(self, request_id: Any = None, **kwargs: Any) -> None:
        """Send the request asynchronously.

        :param request_id: The ID of the request.
        :param kwargs: Additional parameters for the request.
        """
        payload = self._create_payload(request_id, **kwargs)
        await self.client.send(payload.json())

    async def receive(self) -> T:
        """Receive and parse the response asynchronously.

        :return: The parsed response.
        """
        response = await self.client.receive()
        return self._parse_response(response)

    def _create_payload(self, request_id: Any = None, **kwargs: Any) -> Any:
        """Create the request payload.

        :param request_id: The ID of the request.
        :param kwargs: Additional parameters for the request.
        :return: The request payload.
        """
        raise NotImplementedError("Subclasses must implement _create_payload")

    def _parse_response(self, response: dict[str, Any]) -> T:
        """Parse the response.

        This method looks for a method on the corresponding Ogmios class defined
        in the parser_method_name class variable.

        :param response: The response to parse.
        :return: The parsed response.
        """
        parser = getattr(self.ogmios_class, self.parser_method_name)
        return parser(response)  # type: ignore
