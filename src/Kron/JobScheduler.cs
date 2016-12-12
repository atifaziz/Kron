#region Copyright (c) 2016 Atif Aziz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
#endregion

namespace Kron
{
    #region Imports

    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using AngryArrays;
    using AngryArrays.Push;
    using Interlocker;
    using AutoResetEvent = Worms.AutoResetEvent;

    #endregion

    public interface ICancellationTokenProvider
    {
        CancellationToken CancellationToken { get; }
    }

    public class JobEventArgs<T> : EventArgs, ICancellationTokenProvider
    {
        public T Job { get; }
        public CancellationToken CancellationToken { get; }

        public JobEventArgs(T job) :
            this(job, CancellationToken.None) {}

        public JobEventArgs(T job, CancellationToken cancellationToken)
        {
            Job = job;
            CancellationToken = cancellationToken;
        }
    }

    public class JobLifeCycleEventArgs<T> : JobEventArgs<T>
    {
        public JobLifeCycleEventArgs(T job) :
            this(job, CancellationToken.None) {}

        public JobLifeCycleEventArgs(T job, CancellationToken cancellationToken) :
            base(job, cancellationToken) {}
    }

    public class JobStartedEventArgs<T> : JobLifeCycleEventArgs<T>
    {
        public Task Task { get; }
        public DateTime StartTime { get; }

        public JobStartedEventArgs(T job, Task task, DateTime startTime) :
            this(job, task, startTime, CancellationToken.None) {}

        public JobStartedEventArgs(T job, Task task, DateTime startTime, CancellationToken cancellationToken) :
            base(job, cancellationToken)
        {
            Task = task;
            StartTime = startTime;
        }
    }

    public class JobEndedEventArgs<T> : JobLifeCycleEventArgs<T>
    {
        public Task Task { get; }
        public DateTime StartTime { get; }
        public DateTime EndTime { get; }

        public JobEndedEventArgs(T job, Task task, DateTime startTime, DateTime endTime) :
            this(job, task, startTime, endTime, CancellationToken.None) {}

        public JobEndedEventArgs(T job, Task task, DateTime startTime, DateTime endTime, CancellationToken cancellationToken) :
            base(job, cancellationToken)
        {
            Task = task;
            StartTime = startTime;
            EndTime = endTime;
        }
    }

    public enum JobRemovalReason { EndOfSchedule }

    public class JobRemovalEventArgs<T> : JobLifeCycleEventArgs<T>
    {
        public JobRemovalReason Reason { get; }

        public JobRemovalEventArgs(T job, JobRemovalReason reason) :
            this(job, reason, CancellationToken.None) {}

        public JobRemovalEventArgs(T job, JobRemovalReason reason, CancellationToken cancellationToken) :
            base(job, cancellationToken)
        {
            Reason = reason;
        }
    }

    public class JobSchedulerIdleEventArgs<T> : EventArgs, ICancellationTokenProvider
    {
        public CancellationToken CancellationToken { get; }
        public TimeSpan Duration { get; }
        public T NextJob { get; set; }

        public JobSchedulerIdleEventArgs(TimeSpan duration, T nextJob) :
            this(duration, nextJob, CancellationToken.None) {}

        public JobSchedulerIdleEventArgs(TimeSpan duration, T nextJob, CancellationToken cancellationToken)
        {
            Duration = duration;
            NextJob = nextJob;
            CancellationToken = cancellationToken;
        }
    }

    public sealed class JobScheduler<T> : IDisposable
    {
        readonly Interlocked<Job[]> _newJobs = new Interlocked<Job[]>(EmptyArray<Job>.Value);
        readonly AutoResetEvent _newJobsEvent = new AutoResetEvent();
        readonly CancellationTokenSource _cancellationTokenSource;

        CancellationToken CancellationToken => _cancellationTokenSource.Token;
        public Task Task { get; private set; }

        JobScheduler(CancellationToken cancellationToken)
        {
            _cancellationTokenSource =
                cancellationToken.CanBeCanceled
                ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)
                : new CancellationTokenSource();
        }

        public event EventHandler<JobStartedEventArgs<T>> JobStarted;
        public event EventHandler<JobEndedEventArgs<T>> JobEnded;
        public event EventHandler<JobRemovalEventArgs<T>> JobRemoved;
        public event EventHandler<JobSchedulerIdleEventArgs<T>> Idling;

        public static JobScheduler<T> Start() => Start(CancellationToken.None);

        public static JobScheduler<T> Start(CancellationToken cancellationToken) =>
            Start(cancellationToken, null, eventScheduler: null);

        public static JobScheduler<T> Start(CancellationToken cancellationToken,
                                            TaskScheduler jobTaskScheduler,
                                            TaskScheduler eventScheduler) =>
            Start(cancellationToken, jobTaskScheduler, eventScheduler, null, null);

        internal static JobScheduler<T> Start(CancellationToken cancellationToken,
                                              Func<DateTime> clockFunc,
                                              Func<TimeSpan, CancellationToken, Task> delayFunc)
        {
            return Start(cancellationToken, null, null, clockFunc, delayFunc);
        }

        internal static JobScheduler<T> Start(CancellationToken cancellationToken,
                                              TaskScheduler jobTaskScheduler,
                                              TaskScheduler eventScheduler,
                                              Func<DateTime> clockFunc,
                                              Func<TimeSpan, CancellationToken, Task> delayFunc)
        {
            if (clockFunc == null && delayFunc != null)
                throw new ArgumentNullException(nameof(clockFunc));
            if (clockFunc != null && delayFunc == null)
                throw new ArgumentNullException(nameof(delayFunc));

            var scheduler = new JobScheduler<T>(cancellationToken);
            scheduler.Task = scheduler.RunAsync(scheduler.CancellationToken,
                                                jobTaskScheduler ?? TaskScheduler.Default,
                                                eventScheduler ?? TaskScheduler.Current,
                                                clockFunc ?? (() => DateTime.Now),
                                                delayFunc ?? Task.Delay);
            return scheduler;
        }

        async Task RunAsync(CancellationToken cancellationToken,
                            TaskScheduler jobTaskScheduler,
                            TaskScheduler eventScheduler,
                            Func<DateTime> now, Func<TimeSpan,
                            CancellationToken, Task> delay)
        {
            var jobTaskFactory = new TaskFactory(
                cancellationToken, TaskCreationOptions.DenyChildAttach,
                TaskContinuationOptions.None, jobTaskScheduler);

            var events = new
            {
                // The Ignore avoids having to litter code with suppression
                // of CS4014 warning at each call site.

                JobStarted = this.CreateEventAsyncRaiser(me => me.JobStarted, eventScheduler).Ignore(),
                JobEnded   = this.CreateEventAsyncRaiser(me => me.JobEnded  , eventScheduler).Ignore(),
                JobRemoved = this.CreateEventAsyncRaiser(me => me.JobRemoved, eventScheduler).Ignore(),
                Idling     = this.CreateEventAsyncRaiser(me => me.Idling    , eventScheduler).Ignore(),
            };

            var jobs = new List<Job>();
            var runningJobs = new List<RunningJob>();
            var newJobsWaitTask = _newJobsEvent.WaitAsync(cancellationToken);
            var sleepCancellationTokenSource = (CancellationTokenSource) null;
            var sleepTask = (Task<Job>) null;

            while (true)
            {
                var outstandingTasks = new List<Task>(from e in runningJobs select e.Task);
                if (!cancellationToken.IsCancellationRequested)
                {
                    outstandingTasks.Add(newJobsWaitTask);
                    if (sleepTask != null)
                        outstandingTasks.Add(sleepTask);
                }

                if (outstandingTasks.Count == 0)
                    return;

                var completedTask = await Task.WhenAny(outstandingTasks);

                Debug.Assert(completedTask != null, nameof(completedTask) + " != null");

                if (completedTask == newJobsWaitTask)
                {
                    if (!completedTask.IsCanceled)
                        await completedTask;
                    jobs.AddRange(_newJobs.Update(js => Tuple.Create(EmptyArray<Job>.Value, js)));
                    newJobsWaitTask = _newJobsEvent.WaitAsync(cancellationToken);
                }
                else if (completedTask == sleepTask)
                {
                    if (completedTask.IsCanceled)
                        continue;
                    var job = await sleepTask;
                    sleepTask = null;
                    RunJob(job, now(), cancellationToken, jobTaskFactory, runningJobs, events.JobStarted);
                }
                else
                {
                    var i = runningJobs.FindIndex(e => e.Task == completedTask);
                    var endTime = now();
                    var runningJob = runningJobs[i];
                    runningJobs.RemoveAt(i);
                    var job = runningJob.Job;
                    job.LastRunTime = runningJob.StartTime;
                    job.LastEndTime = endTime;
                    events.JobEnded(new JobEndedEventArgs<T>(job.UserObject, runningJob.Task, job.LastRunTime, job.LastEndTime, cancellationToken));
                }

                var nextJobs =
                    from e in jobs
                    where runningJobs.All(rj => rj.Job != e)
                    select new
                    {
                        Job = e,
                        e.LastEndTime,
                        NextRunTime = e.Scheduler(e.UserObject, e.LastEndTime) ?? DateTime.MinValue,
                    }
                    into e
                    orderby e.NextRunTime
                    select e;

                foreach (var e in nextJobs)
                {
                    if (e.NextRunTime < e.LastEndTime)
                    {
                        jobs.Remove(e.Job);
                        events.JobRemoved(new JobRemovalEventArgs<T>(e.Job.UserObject, JobRemovalReason.EndOfSchedule, cancellationToken));
                    }
                    else
                    {
                        var time = now();
                        var duration = e.NextRunTime - time;
                        if (duration.Ticks <= 0)
                        {
                            RunJob(e.Job, time, cancellationToken, jobTaskFactory, runningJobs, events.JobStarted);
                        }
                        else
                        {
                            sleepCancellationTokenSource?.Cancel();
                            sleepCancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                            sleepTask = delay(duration, sleepCancellationTokenSource.Token).ThenReturn(e.Job);
                            events.Idling(new JobSchedulerIdleEventArgs<T>(duration, e.Job.UserObject, cancellationToken));
                            break;
                        }
                    }
                }
            }
        }

        static void RunJob(Job job, DateTime time, CancellationToken cancellationToken, TaskFactory taskFactory, ICollection<RunningJob> runningJobs, Action<JobStartedEventArgs<T>> jobStarted)
        {
            var task = taskFactory.StartNew(() => job.Runner(job.UserObject, cancellationToken), cancellationToken).Unwrap();
            var runningJob = new RunningJob(job, time, task);
            runningJobs.Add(runningJob);
            var args = new JobStartedEventArgs<T>(job.UserObject, runningJob.Task, runningJob.StartTime, cancellationToken);
            jobStarted(args);
        }

        public void Stop() => TryStop(TimeSpan.FromMilliseconds(Timeout.Infinite));

        public void Stop(TimeSpan timeout)
        {
            if (!TryStop(timeout))
                throw new TimeoutException();
        }

        public bool TryStop(TimeSpan timeout)
        {
            _cancellationTokenSource.Cancel();
            // ReSharper disable once MethodSupportsCancellation
            return Task.Wait(timeout);
        }

        void IDisposable.Dispose() => Stop();

        public void AddJob(T job, DateTime time, Func<CancellationToken, Task> runner)
        {
            if (runner == null) throw new ArgumentNullException(nameof(runner));
            AddJob(job, time, (_, ct) => runner(ct));
        }

        public void AddJob(T job, DateTime time, Func<T, CancellationToken, Task> runner) =>
            AddJob(job, (_, dt) => time > dt ? time : default(DateTime?), runner);

        public void AddJob(T job, Func<DateTime, DateTime?> scheduler,
                                  Func<CancellationToken, Task> runner)
        {
            if (scheduler == null) throw new ArgumentNullException(nameof(scheduler));
            if (runner == null) throw new ArgumentNullException(nameof(runner));
            AddJob(job, scheduler, (_, ct) => runner(ct));
        }

        public void AddJob(T job, Func<DateTime, DateTime?> scheduler,
                                  Func<T, CancellationToken, Task> runner)
        {
            if (scheduler == null) throw new ArgumentNullException(nameof(scheduler));
            AddJob(job, (_, dt) => scheduler(dt), runner);
        }

        public void AddJob(T job, Func<T, DateTime, DateTime?> scheduleSelector,
                                  Func<T, CancellationToken, Task> runnerSelector)
        {
            if (scheduleSelector == null) throw new ArgumentNullException(nameof(scheduleSelector));
            if (runnerSelector == null) throw new ArgumentNullException(nameof(runnerSelector));
            _newJobs.Update(js => js.Push(new Job(job, scheduleSelector, runnerSelector)));
            _newJobsEvent.Set();
        }

        sealed class Job
        {
            public readonly T UserObject;
            public readonly Func<T, DateTime, DateTime?> Scheduler;
            public readonly Func<T, CancellationToken, Task> Runner;
            public DateTime LastRunTime;
            public DateTime LastEndTime;

            public Job(T job, Func<T, DateTime, DateTime?> scheduler, Func<T, CancellationToken, Task> runner)
            {
                UserObject = job;
                Scheduler = scheduler;
                Runner = runner;
            }
        }

        sealed class RunningJob
        {
            public readonly Job Job;
            public readonly DateTime StartTime;
            public readonly Task Task;

            public RunningJob(Job job, DateTime startTime, Task task)
            {
                Job = job;
                StartTime = startTime;
                Task = task;
            }
        }
    }
}
