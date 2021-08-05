using System;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Dapper.Contrib.Extensions;
using WorkerContagem.EventHubs;

namespace WorkerContagem.Data
{
    public class ContagemRepository
    {
        private readonly IConfiguration _configuration;

        public ContagemRepository(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public void Save(ContagemEventData resultado)
        {
            using var conexao = new SqlConnection(
                _configuration.GetConnectionString("BaseContagem"));
            conexao.Insert<HistoricoContagem>(new ()
            {
                DataProcessamento = DateTime.UtcNow.AddHours(-3), // Horário padrão do Brasil
                ValorAtual = resultado.ValorAtual,
                Producer = resultado.Producer,
                Consumer = Environment.MachineName,
                EventHub = _configuration["AzureEventHubs:EventHub"],
                Mensagem = resultado.Mensagem,
                Kernel = resultado.Kernel,
                TargetFramework = resultado.TargetFramework
            });
        }
    }
}