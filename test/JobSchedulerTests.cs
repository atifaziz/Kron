namespace Kron.Tests
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Xunit;
    using Semaphore = Worms.Semaphore;

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

        [Fact]
        public async void RunJobOnce()
        {
            var cts = new CancellationTokenSource();
            var clock = new Clock(new DateTime(1970, 1, 1));
            var delay = new Delay();
            var scheduler = JobScheduler<string>.Start(cts.Token, clock.GetCurrentTime, delay.Start);

            var jobStarted = WaitEventAsync<JobStartedEventArgs<string>>(h => scheduler.JobStarted += h, h => scheduler.JobStarted -= h);
            var jobEnded   = WaitEventAsync<JobEndedEventArgs<string>  >(h => scheduler.JobEnded   += h, h => scheduler.JobEnded   -= h);
            var jobRemoved = WaitEventAsync<JobRemovalEventArgs<string>>(h => scheduler.JobRemoved += h, h => scheduler.JobRemoved -= h);

            var ran = false;
            const string id = "foo";
            scheduler.AddJob(id, clock.Time.AddSeconds(1), _ => { ran = true; return Task.FromResult(0); });

            clock.Time += TimeSpan.FromSeconds(1);
            delay.Done();

            await await Task.WhenAny(jobStarted, scheduler.Task);
            Assert.False(scheduler.Task.IsCompleted);

            Assert.Equal(TimeSpan.FromSeconds(1), delay.Duration);
            var jobStartedEventArgs = await jobStarted;
            Assert.NotNull(jobStartedEventArgs);
            Assert.Equal(id, jobStartedEventArgs.Job);
            Assert.Equal(clock.Time, jobStartedEventArgs.StartTime);
            await jobStartedEventArgs.Task;
            Assert.True(ran);

            await await Task.WhenAny(jobEnded, scheduler.Task);
            Assert.False(scheduler.Task.IsCompleted);
            var jobEndedEventArgs = await jobEnded;
            Assert.NotNull(jobEndedEventArgs);
            Assert.Equal(jobStartedEventArgs.Job, jobEndedEventArgs.Job);
            Assert.Equal(jobStartedEventArgs.Task, jobEndedEventArgs.Task);
            Assert.Equal(clock.Time, jobEndedEventArgs.EndTime);

            await await Task.WhenAny(jobRemoved, scheduler.Task);
            Assert.False(scheduler.Task.IsCompleted);
            var jobRemovedEventArgs = await jobRemoved;
            Assert.NotNull(jobRemovedEventArgs);
            Assert.Equal(jobStartedEventArgs.Job, jobRemovedEventArgs.Job);
            Assert.Equal(JobRemovalReason.EndOfSchedule, jobRemovedEventArgs.Reason);

            cts.Cancel();
            await scheduler.Task;
        }

        sealed class Clock
        {
            public DateTime Time;
            public Clock(DateTime time) { Time = time; }
            public DateTime GetCurrentTime() => Time;
        }

        sealed class Delay
        {
            readonly Semaphore _semaphore = new Semaphore();

            public TimeSpan Duration { get; private set; }

            public void Done() => _semaphore.Signal();

            public Task Start(TimeSpan duration, CancellationToken cancellationToken)
            {
                Duration = duration;
                return _semaphore.WaitAsync(cancellationToken);
            }
        }

        static Task<TArgs> WaitEventAsync<TArgs>(Action<EventHandler<TArgs>> adder, Action<EventHandler<TArgs>> remover)
        {
            var tcs = new TaskCompletionSource<TArgs>();
            EventHandler<TArgs>[] handler = { null };
            adder(handler[0] = (sender, args) =>
            {
                remover(handler[0]);
                tcs.SetResult(args);
            });
            return tcs.Task;
        }
    }
}
