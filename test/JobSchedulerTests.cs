namespace Kron.Tests
{
    using System.Threading;
    using Xunit;

    public class JobSchedulerTests
    {
        [Fact]
        public async void Start()
        {
            var cts = new CancellationTokenSource();
            var scheduler = JobScheduler<string>.Start(cts.Token);
            Assert.NotNull(scheduler);
            var task = scheduler.Task;
            Assert.NotNull(task);
            Assert.False(task.IsCompleted);
            cts.Cancel();
            await task;
        }
    }
}
