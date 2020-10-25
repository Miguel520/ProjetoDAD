using System.Threading.Tasks;
using Common.Protos.ProcessCreation;
using PCS.Communications;
using Grpc.Core;
using System;

namespace PCS.Grpc {
    class ProcessCreation : ProcessCreationService.ProcessCreationServiceBase {

        public ProcessCreation() {
        }

        public override Task<CreateServerResponse> CreateServer(CreateServerRequest request, ServerCallContext context) {
            bool success = RequestsDispatcher.CreateServerProcess(GetServerArguments(request));
            if (!success) {
                throw new RpcException(new Status(StatusCode.Internal, "Couldn't creat process."));
            }
            return Task.FromResult(new CreateServerResponse());
        }

        public CreateServerArguments GetServerArguments(CreateServerRequest request) {
            Console.WriteLine("request: " + request);
            return new CreateServerArguments {
                ServerId = request.ServerId,
                Host = request.Host,
                Port = request.Port,
                MinDelay = request.MinDelay,
                MaxDelay = request.MaxDelay
            };
        }

        public override Task<CreateClientResponse> CreateClient(CreateClientRequest request, ServerCallContext context) {
            //TODO
            return base.CreateClient(request, context);
        }

        public CreateClientArguments GetClientArguments(CreateClientRequest request) {
            return new CreateClientArguments {
                Username = request.Username,
                Host = request.Host,
                Script = request.Script
            };
        }
    }
}
