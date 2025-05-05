using Microsoft.AspNetCore.SignalR;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using signalr.backend.Data;
using signalr.backend.Hubs;
using signalr.backend.Models;


namespace signalr.backend.Services
{
    public class PopularChannel : BackgroundService
    {
        public const int DELAY = 30 * 1000;
        public IServiceScopeFactory _serviceScopeFactory;
        public IHubContext<ChatHub> _chathub;
        public PopularChannel(IHubContext<ChatHub> chatHub, IServiceScopeFactory serviceScopeFactory) 
        {
            _chathub = chatHub;
            _serviceScopeFactory = serviceScopeFactory;
        }
        public async Task DoSomething(CancellationToken stoppingToken) 
        {
            using (IServiceScope scope = _serviceScopeFactory.CreateScope())
            {
                ApplicationDbContext dbContext = scope.ServiceProvider.GetRequiredService<ApplicationDbContext>();

                int max = await dbContext.Channel.MaxAsync(c => c.NbMessages);
                Channel channel = await dbContext.Channel.Where(c => c.NbMessages == max).FirstAsync();
                await _chathub.Clients.Group("Channel" + channel.Id).SendAsync("MostPopularChannel", channel.NbMessages,stoppingToken);
            }

        }
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                await Task.Delay(DELAY, stoppingToken);
                await DoSomething(stoppingToken);
            }
        }
    }
}
