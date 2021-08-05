using System;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Processor;
using Azure.Storage.Blobs;
using WorkerContagem.Data;
using WorkerContagem.EventHubs;

namespace WorkerContagem
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly ContagemRepository _repository;
        private readonly int _intervaloMensagemWorkerAtivo;
        private readonly EventProcessorClient _processor;
        private readonly JsonSerializerOptions _jsonSerializerOptions;

        public Worker(ILogger<Worker> logger,
            IConfiguration configuration,
            ContagemRepository repository)
        {
            _logger = logger;
            _repository = repository;
            _intervaloMensagemWorkerAtivo =
                Convert.ToInt32(configuration["IntervaloMensagemWorkerAtivo"]);                        

            var eventHub = configuration["AzureEventHubs:EventHub"];
            var consumerGroup = configuration["AzureEventHubs:ConsumerGroup"];
            var blobContainer = configuration["AzureEventHubs:BlobContainer"];
            
            _processor = new EventProcessorClient(
                new BlobContainerClient(
                    configuration["AzureEventHubs:BlobStorageConnectionString"],
                    blobContainer),
                consumerGroup,
                configuration["AzureEventHubs:EventHubsConnectionString"],
                eventHub);
            _processor.ProcessEventAsync += ProcessEventHandler;
            _processor.ProcessErrorAsync += ProcessErrorHandler;

            _logger.LogInformation($"Event Hub = {eventHub}");
            _logger.LogInformation($"Consumer Group = {consumerGroup}");
            _logger.LogInformation($"Blob Container = {blobContainer}");
            
            _jsonSerializerOptions = new JsonSerializerOptions()
            {
                PropertyNameCaseInsensitive = true
            };
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                await Task.Run(() =>
                {
                    _processor.StartProcessing();
                });

                _logger.LogInformation(
                    $"Worker ativo em: {DateTime.Now:yyyy-MM-dd HH:mm}");
                await Task.Delay(_intervaloMensagemWorkerAtivo, stoppingToken);
            }
        }

        public override Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Executando Stop...");
            _processor.StopProcessing();
            return base.StopAsync(cancellationToken);
        }

        private async Task ProcessEventHandler(ProcessEventArgs eventArgs)
        {
            var eventData = Encoding.UTF8.GetString(eventArgs.Data.Body.ToArray());
            _logger.LogInformation($"[{DateTime.Now:HH:mm:ss} Evento] " + eventData);

            ContagemEventData resultado;            
            try
            {
                resultado = JsonSerializer.Deserialize<ContagemEventData>(
                    eventData, _jsonSerializerOptions);
            }
            catch
            {
                _logger.LogError("Dados inválidos para o Resultado");
                resultado = null;
            }

            if (resultado is not null)
            {
                try
                {
                    _repository.Save(resultado);
                    _logger.LogInformation("Resultado registrado com sucesso!");
                }
                catch (Exception ex)
                {
                    _logger.LogError($"Erro durante a gravação: {ex.Message}");
                }
            }

            await eventArgs.UpdateCheckpointAsync(eventArgs.CancellationToken);
        }

        private Task ProcessErrorHandler(ProcessErrorEventArgs eventArgs)
        {
            _logger.LogError($"Error Handler Exception: {eventArgs.Exception.Message}");
            return Task.CompletedTask;
        }
    }
}