"""Test file for Module Registry Servicer from the server side.

This module contains unit tests for the RegistryServicer class, which handles
module registration and deregistration in the gRPC service.
"""

import secrets
import string

import grpc
import grpc_testing
import pytest
from digitalkin_proto.agentic_mesh_protocol.module_registry.v1 import (
    discover_pb2,
    metadata_pb2,
    module_registry_service_pb2,
    registration_pb2,
    status_pb2,
)

from digitalkin.grpc_servers.registry_servicer import (
    Metadata,
    ModuleStatus,
    RegistryModule,
    RegistryServicer,
)

# Create service instance and get service descriptor for tests
alphabet = string.ascii_letters + string.digits
service_instance = RegistryServicer()
service_name = module_registry_service_pb2.DESCRIPTOR.services_by_name["ModuleRegistryService"]


@pytest.fixture
def module_registry_obj() -> RegistryModule:
    """Create and register a default module in the RegistryServicer.

    This fixture adds a predefined module to the registry for testing purposes.
    The module has type = kin and is in IDLE status.

    Returns:
        RegistryModule: A randomly generated module with unique ID for testing.
    """
    # Generate a random module_id with only letters and numbers
    module_id = "".join(secrets.choice(alphabet) for _ in range(18))

    # Create a test module with predefined properties
    generated_module = RegistryModule(
        module_id=module_id,
        module_type="kin",
        address="127.0.0.1",
        port=50051,
        version="1.0.0",
        message=None,
        metadata=Metadata(name="module", tags=[], description=None),
        status=ModuleStatus.IDLE,
    )

    # Register the module in the service instance
    service_instance.registered_modules[module_id] = generated_module
    return generated_module


@pytest.fixture(scope="module")
def module_registry_objs() -> list[RegistryModule]:
    """Create multiple module entries in the registry.

    This fixture generates a list of modules to test functionality that involves
    multiple registered modules. Each module has a unique ID but identical properties.

    Returns:
        list[RegistryModule]: A list of randomly generated modules for testing.
    """
    modules = []
    for _ in range(15):
        # Generate a random module_id with only letters and numbers
        module_id = "".join(secrets.choice(alphabet) for _ in range(18))

        # Create a test module with predefined properties
        generated_module = RegistryModule(
            module_id=module_id,
            module_type="kin",
            address="127.0.0.1",
            port=50051,
            version="1.0.0",
            message=None,
            metadata=Metadata(name="module", tags=[], description=None),
            status=ModuleStatus.IDLE,
        )

        # Register the module in the service instance
        service_instance.registered_modules[module_id] = generated_module
        modules.append(generated_module)

    return modules


# Test RegisterModule
def test_register_module_success(
    grpc_test_server: grpc_testing.Server,
) -> None:
    """Test successful module registration.

    Verifies that a new module can be registered successfully and that the
    module data is correctly stored in the registry.

    Args:
        grpc_test_server: Mock gRPC server for testing.
    """
    # Create registration request with test module data
    request = registration_pb2.RegisterRequest(
        module_id="module1+",
        module_type="kin",
        address="127.0.0.1",
        port=50051,
        version="1.0.0",
    )

    # Invoke the register module method
    register_module_method = grpc_test_server.invoke_unary_unary(
        method_descriptor=(service_name.methods_by_name["RegisterModule"]),
        invocation_metadata={},
        request=request,
        timeout=1,
    )

    # Get the response
    response, _, code, _ = register_module_method.termination()

    # Test the response status
    assert response.success is True
    assert code == grpc.StatusCode.OK

    # Verify the module was correctly stored in the registry
    module = service_instance.registered_modules.get(request.module_id)
    assert module is not None
    assert module.module_type == request.module_type
    assert module.address == request.address
    assert module.port == request.port
    assert module.version == request.version


# Test RegisterModule
def test_register_module_duplicate(
    grpc_test_server: grpc_testing.Server,
    module_registry_obj: RegistryModule,
) -> None:
    """Test registration of a duplicate module.

    Verifies that attempting to register a module with an ID that already exists
    results in an error response with ALREADY_EXISTS status code.

    Args:
        grpc_test_server: Mock gRPC server for testing.
        module_registry_obj: Pre-registered module fixture for testing duplicates.
    """
    # Try to register a module with an ID that already exists
    # Convert the module object to a request, excluding status and message fields
    request = registration_pb2.RegisterRequest(**{
        k: v for (k, v) in module_registry_obj.model_dump().items() if k not in {"status", "message"}
    })

    # Invoke the register module method
    register_module_method = grpc_test_server.invoke_unary_unary(
        method_descriptor=(service_name.methods_by_name["RegisterModule"]),
        invocation_metadata={},
        request=request,
        timeout=1,
    )

    # Get the response
    response, _, code, details = register_module_method.termination()

    # Verify error response for duplicate module registration
    assert response.success is False
    assert code == grpc.StatusCode.ALREADY_EXISTS
    assert details == f"Module '{request.module_id}' already registered"


# Test DiregisterModule
def test_deregister_module_success(
    grpc_test_server: grpc_testing.Server,
    module_registry_obj: RegistryModule,
) -> None:
    """Test successful module deregistration.

    Verifies that an existing module can be deregistered successfully and is
    removed from the registry.

    Args:
        grpc_test_server: Mock gRPC server for testing.
        module_registry_obj: Pre-registered module fixture to be deregistered.
    """
    # Create deregistration request for existing module
    request = registration_pb2.DeregisterRequest(module_id=module_registry_obj.module_id)

    # Invoke the deregister module method
    deregister_module_method = grpc_test_server.invoke_unary_unary(
        method_descriptor=(service_name.methods_by_name["DeregisterModule"]),
        invocation_metadata={},
        request=request,
        timeout=1,
    )

    # Get the response
    response, _, code, _ = deregister_module_method.termination()

    # Verify successful deregistration
    assert response.success is True
    assert code == grpc.StatusCode.OK

    # Verify the module has been removed from the registry
    module = service_instance.registered_modules.get(request.module_id)
    assert module is None


# Test DiregisterModule
def test_deregister_module_not_found(
    grpc_test_server: grpc_testing.Server,
    module_registry_obj: RegistryModule,
) -> None:
    """Test deregistration of a non-existent module.

    Verifies that attempting to deregister a module that doesn't exist
    results in an error response with NOT_FOUND status code.

    Args:
        grpc_test_server: Mock gRPC server for testing.
        module_registry_obj: Used to generate a non-existent module ID.
    """
    # Create deregistration request for non-existent module
    # Append additional text to ensure the ID doesn't match any existing module
    request = registration_pb2.DeregisterRequest(module_id=f"{module_registry_obj.module_id}+mf_doom")

    # Invoke the deregister module method
    deregister_module_method = grpc_test_server.invoke_unary_unary(
        method_descriptor=(service_name.methods_by_name["DeregisterModule"]),
        invocation_metadata={},
        request=request,
        timeout=1,
    )

    # Get the response
    response, _, code, details = deregister_module_method.termination()

    # Verify error response for non-existent module
    assert response.success is False
    assert code == grpc.StatusCode.NOT_FOUND
    assert details == f"Module {request.module_id} not found in registry"


# Test DiscoverInfoModule
def test_discover_info_module_success(
    grpc_test_server: grpc_testing.Server,
    module_registry_obj: RegistryModule,
) -> None:
    """Test successful module info discovery.

    Verifies that information about a registered module can be retrieved successfully
    and that all module data is correctly returned.

    Args:
        grpc_test_server: Mock gRPC server for testing.
        module_registry_obj: Pre-registered module fixture to be discovered.
    """
    # Create discovery request for existing module
    request = discover_pb2.DiscoverInfoRequest(module_id=module_registry_obj.module_id)

    # Invoke the discover info module method
    discover_info_method = grpc_test_server.invoke_unary_unary(
        method_descriptor=(service_name.methods_by_name["DiscoverInfoModule"]),
        invocation_metadata={},
        request=request,
        timeout=1,
    )

    # Get the response
    response, _, code, _ = discover_info_method.termination()

    # Verify successful discovery
    assert code == grpc.StatusCode.OK

    # Retrieve the module from registry to compare with response
    module = service_instance.registered_modules.get(response.module_id)

    # Verify all module properties are correctly returned
    assert module is not None
    assert module.module_type == module_registry_obj.module_type
    assert module.address == module_registry_obj.address
    assert module.port == module_registry_obj.port
    assert module.version == module_registry_obj.version
    assert module.metadata == module_registry_obj.metadata


# Test DiscoverInfoModule
def test_discover_info_module_not_found(
    grpc_test_server: grpc_testing.Server,
) -> None:
    """Test module info discovery for non-existent module.

    Verifies that attempting to discover information about a module that doesn't exist
    results in an error response with NOT_FOUND status code.

    Args:
        grpc_test_server: Mock gRPC server for testing.
    """
    # Create discovery request for non-existent module
    request = discover_pb2.DiscoverInfoRequest(module_id="+mach-hommy")

    # Invoke the discover info module method
    discover_info_method = grpc_test_server.invoke_unary_unary(
        method_descriptor=(service_name.methods_by_name["DiscoverInfoModule"]),
        invocation_metadata={},
        request=request,
        timeout=1,
    )

    # Get the response
    _, _, code, details = discover_info_method.termination()

    # Verify error response for non-existent module
    assert code == grpc.StatusCode.NOT_FOUND
    assert details == f"Module {request.module_id} not found in registry"


# Test DiscoverSearchModule
def test_discover_search_module_success(
    grpc_test_server: grpc_testing.Server,
    module_registry_objs: list[RegistryModule],
) -> None:
    """Test successful module search by module type.

    Verifies that searching for modules by type returns all matching modules
    with correct information.

    Args:
        grpc_test_server: Mock gRPC server for testing.
        module_registry_objs: List of pre-registered modules for testing search.
    """
    # Create search request by module type
    request = discover_pb2.DiscoverSearchRequest(module_type="kin")

    # Invoke the discover search module method
    discover_search_method = grpc_test_server.invoke_unary_unary(
        method_descriptor=(service_name.methods_by_name["DiscoverSearchModule"]),
        invocation_metadata={},
        request=request,
        timeout=1,
    )

    # Get the response
    response, _, code, _ = discover_search_method.termination()

    # Verify successful search
    assert code == grpc.StatusCode.OK

    # Filter modules in the registry that match the search criteria
    modules = list(
        filter(
            lambda x: request.module_type == x.module_type,
            service_instance.registered_modules.values(),
        )
    )

    # Verify all matching modules are returned
    assert len(response.modules) == len(modules)
    for module in modules:
        # Verify each module is included in the response
        assert any(x for x in response.modules if x.module_id == module.module_id)


# Test DiscoverSearchModule
def test_discover_search_module_success_empty(
    grpc_test_server: grpc_testing.Server,
    module_registry_objs: list[RegistryModule],
) -> None:
    """Test successful module search with no matching tags.

    Verifies that searching for modules with tags that don't match any registered module
    returns an empty list.

    Args:
        grpc_test_server: Mock gRPC server for testing.
        module_registry_objs: List of pre-registered modules without the specific tag.
    """
    # Create search request with a tag that won't match any module
    request = discover_pb2.DiscoverSearchRequest(module_type="kin", tags=[metadata_pb2.Tag(tag="westide_gunn")])

    # Invoke the discover search module method
    discover_search_method = grpc_test_server.invoke_unary_unary(
        method_descriptor=(service_name.methods_by_name["DiscoverSearchModule"]),
        invocation_metadata={},
        request=request,
        timeout=1,
    )

    # Get the response
    response, _, code, _ = discover_search_method.termination()

    # Verify successful search with empty result
    assert code == grpc.StatusCode.OK
    assert len(response.modules) == 0


# Test DiscoverSearchModule
def test_discover_search_module_success_no_match(
    grpc_test_server: grpc_testing.Server,
) -> None:
    """Test successful module search with no matching module type.

    Verifies that searching for modules with a module type that doesn't match any
    registered module returns an empty list.

    Args:
        grpc_test_server: Mock gRPC server for testing.
    """
    # Create search request with a module type that won't match any module
    request = discover_pb2.DiscoverSearchRequest(module_type="trigger")

    # Invoke the discover search module method
    discover_search_method = grpc_test_server.invoke_unary_unary(
        method_descriptor=(service_name.methods_by_name["DiscoverSearchModule"]),
        invocation_metadata={},
        request=request,
        timeout=1,
    )

    # Get the response
    response, _, code, _ = discover_search_method.termination()

    # Verify successful search with empty result
    assert code == grpc.StatusCode.OK
    assert len(response.modules) == 0


# Test GetModuleStatus
def test_get_module_status_success(
    grpc_test_server: grpc_testing.Server,
    module_registry_obj: RegistryModule,
) -> None:
    """Test successful query module status.

    Verifies that a new module can be registered successfully and that the
    module data is correctly stored in the registry.

    Args:
        grpc_test_server: Mock gRPC server for testing.
        module_registry_obj: Pre-registered module.
    """
    request_get_module = status_pb2.ModuleStatusRequest(module_id=module_registry_obj.module_id)
    # Invoke the get module status method
    get_module_method = grpc_test_server.invoke_unary_unary(
        method_descriptor=(service_name.methods_by_name["GetModuleStatus"]),
        invocation_metadata={},
        request=request_get_module,
        timeout=1,
    )

    # Get the response
    response, _, code, _ = get_module_method.termination()

    # Test the response status
    assert code == grpc.StatusCode.OK

    # Verify the module status is correctly queried in the registry
    assert response.module_id == module_registry_obj.module_id
    assert response.status == module_registry_obj.status.value


# Test GetModuleStatus
def test_get_module_not_found(
    grpc_test_server: grpc_testing.Server,
    module_registry_obj: RegistryModule,
) -> None:
    """Test registration of a duplicate module.

    Verifies that attempting to register a module with an ID that already exists
    results in an error response with ALREADY_EXISTS status code.

    Args:
        grpc_test_server: Mock gRPC server for testing.
        module_registry_obj: Pre-registered module.
    """
    module_id = "kungfu_kenny"
    request_get_module = status_pb2.ModuleStatusRequest(module_id=module_id)
    # Invoke the get module status method
    get_module_method = grpc_test_server.invoke_unary_unary(
        method_descriptor=(service_name.methods_by_name["GetModuleStatus"]),
        invocation_metadata={},
        request=request_get_module,
        timeout=1,
    )

    # Get the response
    _, _, code, details = get_module_method.termination()

    # Verify error response for non-existent module
    assert code == grpc.StatusCode.NOT_FOUND
    assert details == f"Module {module_id} not found in registry"


# Test ListModuleStatus
@pytest.mark.parametrize(
    ("list_size", "offset"),
    [
        (100, 0),
        (1, 10),
        (1000000, 0),
    ],
    ids=["list_size", "low_list_size_offset", "list_size_max"],
)
def test_list_module_status_success_pagination(
    grpc_test_server: grpc_testing.Server,
    module_registry_objs: list[RegistryModule],
    list_size: int,
    offset: int,
) -> None:
    """Test successful list modules with pagination.

    Verifies that queries for modules following query parameter.

    Args:
        grpc_test_server: Mock gRPC server for testing.
        module_registry_objs: List of pre-registered modules without the specific tag.
        list_size: query parameter
        offset: query parameter
    """
    # Create search request with a tag that won't match any module
    request = status_pb2.ListModulesStatusRequest(list_size=list_size, offset=offset)

    # Invoke the discover search module method
    list_method = grpc_test_server.invoke_unary_unary(
        method_descriptor=(service_name.methods_by_name["ListModuleStatus"]),
        invocation_metadata={},
        request=request,
        timeout=1,
    )

    # Get the response
    response, _, code, _ = list_method.termination()

    # Verify successful search with empty result
    assert code == grpc.StatusCode.OK

    assert response.list_size <= list_size
    # Filter modules in the registry that match the search criteria
    modules = list(service_instance.registered_modules.values())[offset : offset + list_size]

    for response_module, expected_module in zip(response.modules_statuses, modules):
        # Verify each module is included in the response
        assert response_module.status == expected_module.status.value


# Test ListModuleStatus
def test_list_module_status_success(
    grpc_test_server: grpc_testing.Server,
    module_registry_objs: list[RegistryModule],
) -> None:
    """Test successful list all modules.

    Verifies that queries for modules.

    Args:
        grpc_test_server: Mock gRPC server for testing.
        module_registry_objs: List of pre-registered modules without the specific tag.
    """
    # Create search request with a tag that won't match any module
    request = status_pb2.ListModulesStatusRequest()

    # Invoke the discover search module method
    list_method = grpc_test_server.invoke_unary_unary(
        method_descriptor=(service_name.methods_by_name["ListModuleStatus"]),
        invocation_metadata={},
        request=request,
        timeout=1,
    )

    # Get the response
    response, _, code, _ = list_method.termination()

    # Verify successful search with empty result
    assert code == grpc.StatusCode.OK

    # Filter modules in the registry that match the search criteria
    modules = list(service_instance.registered_modules.values())
    # Verify all matching modules are returned
    assert len(response.modules_statuses) == len(modules)

    for response_module, expected_module in zip(response.modules_statuses, modules):
        # Verify each module is included in the response
        assert response_module.status == expected_module.status.value


def test_get_all_module_status_success(
    grpc_test_server: grpc_testing.Server,
    module_registry_objs: list[RegistryModule],
) -> None:
    """Test successful get a stream of all modules.

    Verifies that stream for all modules matches the registry.

    Args:
        grpc_test_server: Mock gRPC server for testing.
        module_registry_objs: List of pre-registered modules without the specific tag.
    """
    # Create search request with a tag that won't match any module
    request = status_pb2.GetAllModulesStatusRequest()

    # Invoke the discover search module method
    get_all_method = grpc_test_server.invoke_unary_stream(
        method_descriptor=(service_name.methods_by_name["GetAllModuleStatus"]),
        invocation_metadata={},
        request=request,
        timeout=1,
    )

    # Get the response
    _, code, _ = get_all_method.termination()

    # Verify successful search with empty result
    assert code == grpc.StatusCode.OK

    response_modules = []
    try:
        while True:
            response_modules.append(get_all_method.take_response())
    except ValueError:
        pass

    # Filter modules in the registry that match the search criteria
    modules = list(service_instance.registered_modules.values())
    # Verify all matching modules are returned
    assert len(response_modules) == len(modules)

    for response_module, expected_module in zip(response_modules, modules):
        # Verify each module is included in the response
        assert response_module.status == expected_module.status.value


# Test UpdateModuleStatus
@pytest.mark.parametrize(
    ("module", "expected_status"),
    [
        (
            RegistryModule(
                module_id="module_running",
                module_type="kin",
                address="127.0.0.1",
                port=50051,
                version="1.0.0",
                metadata=Metadata(name="", tags=[], description=None),
                message=None,
                status=ModuleStatus.RUNNING,
            ),
            ModuleStatus.RUNNING,
        ),
        (
            RegistryModule(
                module_id="module_idle",
                module_type="kin",
                address="127.0.0.1",
                port=50051,
                version="1.0.0",
                metadata=Metadata(name="", tags=[], description=None),
                message=None,
                status=ModuleStatus.IDLE,
            ),
            ModuleStatus.IDLE,
        ),
        (
            RegistryModule(
                module_id="module_ended",
                module_type="kin",
                address="127.0.0.1",
                port=50051,
                version="1.0.0",
                metadata=Metadata(name="", tags=[], description=None),
                message=None,
                status=ModuleStatus.ENDED,
            ),
            ModuleStatus.ENDED,
        ),
    ],
    ids=["running", "idle", "ended"],
)
def test_update_module_success(
    grpc_test_server: grpc_testing.Server,
    module: RegistryModule,
    expected_status: ModuleStatus,
) -> None:
    """Test successful update module status.

    Verifies that a module can be updated successfully and that the
    module data is correctly stored in the registry.

    Args:
        grpc_test_server: Mock gRPC server for testing.
        module: Fixture representation of a RegistryModule.
        expected_status: Fixture of the expected ModuleStatus.
    """
    # Create registration request with test module data
    request_register = registration_pb2.RegisterRequest(**{
        k: v for (k, v) in module.model_dump().items() if k not in {"status", "message"}
    })

    # Invoke the register module method
    register_module_method = grpc_test_server.invoke_unary_unary(
        method_descriptor=(service_name.methods_by_name["RegisterModule"]),
        invocation_metadata={},
        request=request_register,
        timeout=1,
    )

    # Get the response
    response, _, code, _ = register_module_method.termination()

    # Test the response status
    assert response.success is True
    assert code == grpc.StatusCode.OK

    request_update_module = status_pb2.UpdateStatusRequest(module_id=module.module_id, status=module.status.name)
    # Invoke the get module status method
    update_module_method = grpc_test_server.invoke_unary_unary(
        method_descriptor=(service_name.methods_by_name["UpdateModuleStatus"]),
        invocation_metadata={},
        request=request_update_module,
        timeout=1,
    )

    # Get the response
    response, _, code, _ = update_module_method.termination()

    # Test the response status
    assert response.success is True
    assert code == grpc.StatusCode.OK

    request_get_module = status_pb2.ModuleStatusRequest(module_id=module.module_id)
    # Invoke the get module status method
    get_module_method = grpc_test_server.invoke_unary_unary(
        method_descriptor=(service_name.methods_by_name["GetModuleStatus"]),
        invocation_metadata={},
        request=request_get_module,
        timeout=1,
    )

    # Get the response
    response, _, code, _ = get_module_method.termination()

    # Test the response status
    assert code == grpc.StatusCode.OK

    # Verify the module status is correctly queried in the registry
    assert response.module_id == module.module_id
    assert response.status == expected_status.value


# Test UpdateModuleStatus
@pytest.mark.parametrize(
    ("module", "error_status"),
    [
        (
            RegistryModule(
                module_id="module_outside_range",
                module_type="kin",
                address="127.0.0.1",
                port=50051,
                version="1.0.0",
                metadata=Metadata(name="", tags=[], description=None),
                message=None,
                status=ModuleStatus.RUNNING,
            ),
            5,
        ),
        (
            RegistryModule(
                module_id="module_negative",
                module_type="kin",
                address="127.0.0.1",
                port=50051,
                version="1.0.0",
                metadata=Metadata(name="", tags=[], description=None),
                message=None,
                status=ModuleStatus.ENDED,
            ),
            -1,
        ),
    ],
    ids=["outside_range", "negative"],
)
def test_update_module_error(grpc_test_server: grpc_testing.Server, module: RegistryModule, error_status: int) -> None:
    """Test successful update module status.

    Verifies that a module can be updated successfully and that the
    module data is correctly stored in the registry.

    Args:
        grpc_test_server: Mock gRPC server for testing.
        module: Fixture representation of a RegistryModule.
        error_status: Fixture of the expected error ModuleStatus.
    """
    # Create registration request with test module data
    request_register = registration_pb2.RegisterRequest(**{
        k: v for (k, v) in module.model_dump().items() if k not in {"status", "message"}
    })

    # Invoke the register module method
    register_module_method = grpc_test_server.invoke_unary_unary(
        method_descriptor=(service_name.methods_by_name["RegisterModule"]),
        invocation_metadata={},
        request=request_register,
        timeout=1,
    )

    # Get the response
    response, _, code, _ = register_module_method.termination()

    # Test the response status
    assert response.success is True
    assert code == grpc.StatusCode.OK

    request_update_module = status_pb2.UpdateStatusRequest(module_id=module.module_id)
    request_update_module.status = error_status  # type: ignore

    # Invoke the get module status method
    update_module_method = grpc_test_server.invoke_unary_unary(
        method_descriptor=(service_name.methods_by_name["UpdateModuleStatus"]),
        invocation_metadata={},
        request=request_update_module,
        timeout=1,
    )

    # with pytest.raises(grpc.RpcError) as exc_info:
    # Get the response
    _, _, code, details = update_module_method.termination()

    # Test the response status
    assert code == grpc.StatusCode.INVALID_ARGUMENT
    assert details == f"ModuleStatus {error_status} is unknonw, please check the requested status"
