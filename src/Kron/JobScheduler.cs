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

    public class JobEventArgs<T> : EventArgs
    {
        public T Job { get; }
        public JobEventArgs(T job) { Job = job; }
    }

    public class JobLifeCycleEventArgs<T> : JobEventArgs<T>
    {
        public JobLifeCycleEventArgs(T job) : base(job) {}
    }

    public class JobStartedEventArgs<T> : JobLifeCycleEventArgs<T>
    {
        public Task Task { get; }
        public DateTime StartTime { get; }

        public JobStartedEventArgs(T job, Task task, DateTime startTime) :
            base(job)
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
            base(job)
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

        public JobRemovalEventArgs(T job, JobRemovalReason reason) : base(job)
        {
            Reason = reason;
        }
    }

    public class JobSchedulerIdleEventArgs<T> : EventArgs
    {
        public TimeSpan Duration { get; }
        public T NextJob { get; set; }

        public JobSchedulerIdleEventArgs(TimeSpan duration, T nextJob)
        {
            Duration = duration;
            NextJob = nextJob;
        }
    }

    public sealed class JobScheduler<T>
    {
        public event EventHandler<JobStartedEventArgs<T>> JobStarted;
        public event EventHandler<JobEndedEventArgs<T>> JobEnded;
        public event EventHandler<JobRemovalEventArgs<T>> JobRemoved;
        public event EventHandler<JobSchedulerIdleEventArgs<T>> Idling;

        readonly Interlocked<Job[]> _newJobs = new Interlocked<Job[]>(EmptyArray<Job>.Value);
        readonly AutoResetEvent _newJobsEvent = new AutoResetEvent();

        public Task Task { get; private set; }

        JobScheduler() { }

        public static JobScheduler<T> Start(CancellationToken cancellationToken) =>
            Start(cancellationToken, null, null);

        public static JobScheduler<T> Start(CancellationToken cancellationToken,
                                            Func<DateTime> clockFunc,
                                            Func<TimeSpan, CancellationToken, Task> delayFunc)
        {
            if (clockFunc == null && delayFunc != null)
                throw new ArgumentNullException(nameof(clockFunc));
            if (clockFunc != null && delayFunc == null)
                throw new ArgumentNullException(nameof(delayFunc));

            var scheduler = new JobScheduler<T>();
            scheduler.Task = scheduler.RunAsync(cancellationToken,
                                                clockFunc ?? (() => DateTime.Now),
                                                delayFunc ?? Task.Delay);
            return scheduler;
        }

        async Task RunAsync(CancellationToken cancellationToken,
                            Func<DateTime> now, Func<TimeSpan,
                            CancellationToken, Task> delay)
        {
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
                if (completedTask.IsCanceled)
                    continue;

                if (completedTask == newJobsWaitTask)
                {
                    await completedTask;
                    jobs.AddRange(_newJobs.Update(js => Tuple.Create(EmptyArray<Job>.Value, js)));
                    newJobsWaitTask = _newJobsEvent.WaitAsync(cancellationToken);
                }
                else if (completedTask == sleepTask)
                {
                    var job = await sleepTask;
                    sleepTask = null;
                    RunJob(job, now(), cancellationToken, runningJobs);
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
                    JobEnded?.Invoke(this, new JobEndedEventArgs<T>(job.UserObject, runningJob.Task, job.LastRunTime, job.LastEndTime));
                }

                var nextJobs =
                    from e in jobs
                    where runningJobs.All(rj => rj.Job != e)
                    select new
                    {
                        Job = e,
                        e.LastEndTime,
                        NextRunTime = e.Scheduler(e.LastEndTime) ?? DateTime.MinValue,
                    }
                    into e
                    orderby e.NextRunTime
                    select e;

                foreach (var e in nextJobs)
                {
                    if (e.NextRunTime < e.LastEndTime)
                    {
                        jobs.Remove(e.Job);
                        JobRemoved?.Invoke(this, new JobRemovalEventArgs<T>(e.Job.UserObject, JobRemovalReason.EndOfSchedule));
                    }
                    else
                    {
                        var time = now();
                        var duration = e.NextRunTime - time;
                        if (duration.Ticks <= 0)
                        {
                            RunJob(e.Job, time, cancellationToken, runningJobs);
                        }
                        else
                        {
                            sleepCancellationTokenSource?.Cancel();
                            sleepCancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                            sleepTask = delay(duration, sleepCancellationTokenSource.Token).ThenReturn(e.Job);
                            Idling?.Invoke(this, new JobSchedulerIdleEventArgs<T>(duration, e.Job.UserObject));
                            break;
                        }
                    }
                }
            }
        }

        void RunJob(Job job, DateTime time, CancellationToken cancellationToken, ICollection<RunningJob> runningJobs)
        {
            var runningJob = new RunningJob(job, time, job.Runner(cancellationToken));
            runningJobs.Add(runningJob);
            var args = new JobStartedEventArgs<T>(job.UserObject, runningJob.Task, runningJob.StartTime);
            JobStarted?.Invoke(this, args);
        }

        public void AddJob(T job, Func<T, Func<DateTime, DateTime?>> scheduleSelector,
                                  Func<T, Func<CancellationToken, Task>> runnerSelector)
        {
            if (scheduleSelector == null) throw new ArgumentNullException(nameof(scheduleSelector));
            if (runnerSelector == null) throw new ArgumentNullException(nameof(runnerSelector));
            AddJob(job, scheduleSelector(job), runnerSelector(job));
        }

        public void AddJob(T job, DateTime time, Func<CancellationToken, Task> runner) =>
            AddJob(job, dt => time > dt ? time : default(DateTime?), runner);

        public void AddJob(T job, Func<DateTime, DateTime?> scheduler,
                                  Func<CancellationToken, Task> runner)
        {
            if (scheduler == null) throw new ArgumentNullException(nameof(scheduler));
            if (runner == null) throw new ArgumentNullException(nameof(runner));
            AddJob(new Job(job, scheduler, runner));
        }

        void AddJob(Job job)
        {
            _newJobs.Update(js => js.Push(job));
            _newJobsEvent.Set();
        }

        sealed class Job
        {
            public readonly T UserObject;
            public readonly Func<DateTime, DateTime?> Scheduler;
            public readonly Func<CancellationToken, Task> Runner;
            public DateTime LastRunTime;
            public DateTime LastEndTime;

            public Job(T job, Func<DateTime, DateTime?> scheduler, Func<CancellationToken, Task> runner)
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
