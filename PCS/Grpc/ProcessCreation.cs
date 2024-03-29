﻿using Grpc.Core;
using Common.Protos.ProcessCreation;
using System.Linq;
using System.Threading.Tasks;

using PCS.Communications;

namespace PCS.Grpc {
    class ProcessCreation : ProcessCreationService.ProcessCreationServiceBase {

        public ProcessCreation() {
        }

        public override Task<CreateServerResponse> CreateServer(CreateServerRequest request, ServerCallContext context) {
            bool success = RequestsDispatcher.CreateServerProcess(GetServerArguments(request));
            if (!success) {
                throw new RpcException(new Status(StatusCode.Internal, "Couldn't create server process."));
            }
            return Task.FromResult(new CreateServerResponse());
        }

        public CreateServerArguments GetServerArguments(CreateServerRequest request) {
            return new CreateServerArguments {
                ServerId = request.ServerId,
                Host = request.Host,
                Port = request.Port,
                MinDelay = request.MinDelay,
                MaxDelay = request.MaxDelay,
                Version = request.Version
            };
        }

        public override Task<CreateClientResponse> CreateClient(CreateClientRequest request, ServerCallContext context) {
            bool success = RequestsDispatcher.CreateClientProcess(GetClientArguments(request));
            if (!success) {
                throw new RpcException(new Status(StatusCode.Internal, "Couldn't create client process."));
            }
            return Task.FromResult(new CreateClientResponse());
        }

        public CreateClientArguments GetClientArguments(CreateClientRequest request) {
            return new CreateClientArguments {
                Username = request.Username,
                Host = request.Host,
                Port = request.Port,
                Script = request.Script,
                NameServersUrls = request.NamingServersUrls.ToList(),
                Version = request.Version
            };
        }
    }
}
