<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->
<template>
  <el-card>
    <!--Title-->
    <h1>Nemo Jobs</h1>

    <!--Jobs information-->
    <p>
      <b>User: </b><span>{ TODO }</span><br>
      <b>Total Uptime: </b><span>{ TODO }</span><br>
      <b>Scheduling Mode: </b><span>{ TODO }</span><br>
      <b @click="jump($event, JOB_STATUS.RUNNING)"><a>
        Active Jobs: </a></b><el-badge type="primary" :value="activeJobsData.length"></el-badge><br>
      <b @click="jump($event, JOB_STATUS.COMPLETE)"><a>
        Completed Jobs: </a></b><el-badge type="success" :value="completedJobsData.length"></el-badge><br>
      <b @click="jump($event, JOB_STATUS.FAILED)"><a>
        Failed Jobs: </a></b><el-badge type="danger" :value="failedJobsData.length"></el-badge><br>
    </p>

    <!--Stage Timeline-->
    <el-collapse>
      <el-collapse-item title="Event Timeline" name="1">
        { TODO: JOBS TIMELINE }
      </el-collapse-item>
    </el-collapse>

    <!--Jobs list-->
    <h2 ref="activeJobs">Active Jobs
      <el-badge type="primary" :value="activeJobsData.length"></el-badge></h2>
    <div v-if="activeJobsData.length !== 0">
      <!--TODO: 이거 component로 refactor 하기-->
      <el-table class="active-jobs-table" :data="activeJobsData"
                @row-click="handleSelect" stripe>
        <el-table-column label="Job id" width="100">
          <template slot-scope="scope">
            {{ _getFrom(scope.row.jobId) }}
          </template>
        </el-table-column>
        <el-table-column label="Description" width="180"></el-table-column>
        <el-table-column label="Submitted" width="180"></el-table-column>
        <el-table-column label="Duration" width="90"></el-table-column>
        <el-table-column label="Stages: Succeeded/Total" width="200"></el-table-column>
        <el-table-column label="Tasks (for all stages): Succeeded/Total"></el-table-column>
        <!--<el-table-column label="Status">-->
        <!--<template slot-scope="scope">-->
        <!--<el-tag :type="_fromJobStatusToType(scope.row.status)">-->
        <!--{{ scope.row.status }}-->
        <!--</el-tag>-->
        <!--</template>-->
        <!--</el-table-column>-->
        <!--<el-table-column label="Operations">-->
        <!--<template slot-scope="scope">-->
        <!--<el-button-->
        <!--@click="selectJobId(scope.row.jobId)"-->
        <!--round-->
        <!--type="primary">-->
        <!--Select-->
        <!--</el-button>-->
        <!--<el-button-->
        <!--@click="deleteJobId(scope.row.jobId)"-->
        <!--circle-->
        <!--type="danger"-->
        <!--icon="el-icon-delete"/>-->
        <!--<el-button-->
        <!--v-if="_isWebSocketJob(scope.row.jobId)"-->
        <!--@click="prepareWebSocket(scope.row.jobId)"-->
        <!--:disabled="_reconnectDisabled(scope.row.status)"-->
        <!--circle-->
        <!--type="info"-->
        <!--:icon="_reconnectIconType(scope.row.status)"/>-->
        <!--</template>-->
        <!--</el-table-column>-->
      </el-table>
    </div>

    <h2 ref="completedJobs">Completed Jobs
      <el-badge type="success" :value="completedJobsData.length"></el-badge></h2>
    <div v-if="completedJobsData.length !== 0">
      <el-table class="completed-jobs-table" :data="completedJobsData"
                @row-click="handleSelect" stripe>
        <el-table-column label="Job id" width="100">
          <template slot-scope="scope">
            {{ _getFrom(scope.row.jobId) }}
          </template>
        </el-table-column>
        <el-table-column label="Description" width="180"></el-table-column>
        <el-table-column label="Submitted" width="180"></el-table-column>
        <el-table-column label="Duration" width="90"></el-table-column>
        <el-table-column label="Stages: Succeeded/Total" width="200"></el-table-column>
        <el-table-column label="Tasks (for all stages): Succeeded/Total"></el-table-column>
      </el-table>
    </div>

    <h2 ref="failedJobs">Failed Jobs
      <el-badge type="danger" :value="failedJobsData.length"></el-badge></h2>
    <div v-if="failedJobsData.length !== 0">
      <el-table class="failed-jobs-table" :data="failedJobsData"
                @row-click="handleSelect" stripe>
        <el-table-column label="Job id" width="100">
          <template slot-scope="scope">
            {{ _getFrom(scope.row.jobId) }}
          </template>
        </el-table-column>
        <el-table-column label="Description" width="180"></el-table-column>
        <el-table-column label="Submitted" width="180"></el-table-column>
        <el-table-column label="Duration" width="90"></el-table-column>
        <el-table-column label="Stages: Succeeded/Total" width="200"></el-table-column>
        <el-table-column label="Tasks (for all stages): Succeeded/Total"></el-table-column>
      </el-table>
    </div>


    <!--Add job button-->
    <el-button :disabled="uploading" icon="el-icon-plus" plain
               @click="addJobDialogVisible = true" style="margin: auto;">
    </el-button>

    <!--Add Job dialog-->
    <el-dialog
      title="Add job"
      :visible.sync="addJobDialogVisible"
      width="40%">
      <el-card class="dialog-card">
        <div slot="header">
          <h2>From JSON file</h2>
        </div>
        <el-row type="flex" justify="center">
          <el-upload
            drag
            :span="20"
            action=""
            :http-request="handleUpload"
            :auto-upload="true"
            :show-file-list="false">
            <i class="el-icon-upload"/>
            <div class="el-upload__text">
              Drop JSON here or <em>click here to upload.</em>
            </div>
          </el-upload>
        </el-row>
      </el-card>
      <el-card>
        <div slot="header">
          <h2>From WebSocket endpoint</h2>
        </div>
        <el-row class="dialog-row">
          <el-input
            clearable
            @keyup.enter.native="handleWebSocketAdd"
            placeholder="WebSocket endpoint"
            v-model="wsEndpointInput"/>
        </el-row>
        <el-row class="dialog-row" type="flex" justify="end">
          <el-button
            type="primary"
            plain
            @click="handleWebSocketAdd">
            Add
          </el-button>
        </el-row>
      </el-card>
    </el-dialog>

    <br><br><br>

    <!--Selected Job-->
    <job-view :selected-job-status="selectedJobStatus" :selected-job-metric-data-set="selectedJobMetricDataSet"/>
    <!--<job-view :selectedJobId="selectedJobId"/>-->
  </el-card>
</template>

<script>
import Vue from 'vue';
import JobView from './detail/JobView';
import uuid from 'uuid/v4';
import { DataSet } from 'vue2vis';
import { STATE, JOB_STATUS } from '../../assets/constants';

function _isDone(status) {
  return status === JOB_STATUS.COMPLETE ||
    status === JOB_STATUS.FAILED;
}

export default {
  components: {
    JobView,
    'job-view': JobView,
  },

  data() {
    return {
      JOB_STATUS: JOB_STATUS,
      // job id -> job data object
      jobs: {},

      selectedJobId: '',
      selectedJobMetricDataSet: [],

      // ui-specific
      addJobDialogVisible: false,
      uploading: false,

      wsEndpointInput: '',
    };
  },

  //COMPUTED
  computed: {
    /**
     * Computed property of table rows, containing job id and status.
     */
    jobTableData() {
      return Object.keys(this.jobs).map(jobId => ({
        jobId: jobId,
        status: this.jobs[jobId].status,
      }));
    },
    activeJobsData() {
      return Object.keys(this.jobs).filter(jobId => this.jobs[jobId].status === JOB_STATUS.RUNNING).map(jobId => ({
        jobId: jobId,
        status: this.jobs[jobId].status,
      }));
    },
    completedJobsData() {
      return Object.keys(this.jobs).filter(jobId => this.jobs[jobId].status === JOB_STATUS.COMPLETE).map(jobId => ({
        jobId: jobId,
        status: this.jobs[jobId].status,
      }));
    },
    failedJobsData() {
      return Object.keys(this.jobs).filter(jobId => this.jobs[jobId].status === JOB_STATUS.FAILED).map(jobId => ({
        jobId: jobId,
        status: this.jobs[jobId].status,
      }));
    },
    selectedJobStatus() {
      if (this.selectedJobId !== '') {
        return this.jobs[this.selectedJobId].status;
      } else {
        return '';
      }
    }
  },

  //METHODS
  methods: {
    /**
     * Handler for uploading file.
     */
    async handleUpload(options) {
      this.addJobDialogVisible = false;
      let fileReader = new FileReader();

      fileReader.onload = event => {
        this.uploading = false;
        this.addJobFromFile(options.file.name, event.target.result);
      };

      fileReader.onerror = event => {
        this.uploading = true;
        this.notifyError('File upload failed');
      };

      this.uploading = true;
      fileReader.readAsText(options.file);
    },

    /**
     * Show notification on right side of screen
     * using element-ui's notification function.
     */
    notifyError(msg) {
      this.$notify.error({
        title: 'Error',
        message: msg,
      });
    },

    // Handle selection of a job
    handleSelect(val) {
      if (this.selectedJobId !== val.jobId) {
        this.selectJobId(val.jobId);
      }
    },

    // jump to the table of jobs
    jump(event, val) {
      switch (val) {
        case JOB_STATUS.RUNNING:
          this.$refs.activeJobs.scrollIntoView();
          break;
        case JOB_STATUS.COMPLETE:
          this.$refs.completedJobs.scrollIntoView();
          break;
        case JOB_STATUS.FAILED:
          this.$refs.failedJobs.scrollIntoView();
          break;
      }
    },

    _getFrom(jobId) {
      const job = this.jobs[jobId];
      return job.endpoint ? job.endpoint : job.fileName;
    },

    _isWebSocketJob(jobId) {
      return this.jobs[jobId].endpoint ? true : false;
    },

    _reconnectDisabled(status) {
      switch (status) {
        case JOB_STATUS.NOT_CONNECTED:
          return false;
        case JOB_STATUS.CONNECTING:
          return true;
        default:
          return true;
      }
    },

    _reconnectIconType(status) {
      switch (status) {
        case JOB_STATUS.NOT_CONNECTED:
          return 'el-icon-refresh';
        case JOB_STATUS.CONNECTING:
          return 'el-icon-loading';
        default:
          return 'el-icon-refresh';
      }
    },

    /**
     * Select job and propagate job change event to other components.
     * @param jobId id of job.
     */
    async selectJobId(jobId) {
      if (!(jobId in this.jobs) || jobId === this.selectedJobId) {
        return;
      }

      this.selectedJobId = jobId;
      const job = this.jobs[jobId];
      this.selectedJobMetricDataSet = job.metricDataSet;
      this.$eventBus.$emit('job-id-select', {
        jobId,
        jobFrom: this._getFrom(jobId),
        metricLookupMap: job.metricLookupMap,
      });

      await this.$nextTick();

      if (job.dag) {
        this.$eventBus.$emit('dag', {
          dag: job.dag,
          jobId,
          init: true,
          states: job.dagStageState,
        });
        this.$eventBus.$emit('rerender-dag');
      }
    },

    /**
     * Delete job and propagate job deletion events to other components.
     * @param jobId id of job.
     */
    deleteJobId(jobId) {
      if (this.selectedJobId === jobId) {
        this.selectedJobId = '';
        this.$eventBus.$emit('job-id-deselect');
        this.$eventBus.$emit('clear-dag');
      }
      const job = this.jobs[jobId];

      if (job.ws) {
        ws.close();
      }

      Vue.delete(this.jobs, jobId);
    },

    /**
     * Handler for adding WebSocket endpoint.
     */
    async handleWebSocketAdd() {
      await this.addJobFromWebSocketEndpoint(this.wsEndpointInput);
      this.wsEndpointInput = '';
      this.addJobDialogVisible = false;
    },

    _newJob(jobId) {
      Vue.set(this.jobs, jobId, {
        ws: undefined,
        endpoint: '',
        fileName: '',
        dag: undefined,
        metricLookupMap: {},
        metricDataSet: new DataSet([]),
        dagStageState: {},
        status: '',
      });
    },

    /**
     * Method for parsing JSON dump file and processing
     * inner metric information.
     * @param fileName name of JSON file.
     * @param content content of file.
     */
    async addJobFromFile(fileName, content) {
      let parsedData;
      try {
        parsedData = JSON.parse(content);
      } catch (e) {
        this.notifyError('Invalid JSON file');
        return;
      }

      // for now, Nemo's job id is not informative,
      // so job id is generated with uuid.
      const jobId = uuid();
      this._newJob(jobId);
      this.jobs[jobId].fileName = fileName;
      this.jobs[jobId].status = JOB_STATUS.RUNNING;
      this.selectJobId(jobId);

      await this.$nextTick();
      this.processMetric(parsedData, jobId);
    },

    /**
     * Add job from WebSocket endpoint.
     * @param endpoint WebSocket endpoint.
     */
    async addJobFromWebSocketEndpoint(endpoint) {
      let alreadyExistsError = false;
      endpoint = endpoint.trim();

      Object.keys(this.jobs)
        .filter(k => _isDone(!this.jobs[k].status))
        .forEach(k => {
          if (this.jobs[k].endpoint === endpoint) {
            alreadyExistsError = true;
            return;
          }
        });

      if (alreadyExistsError) {
        this.notifyError('Endpoint already exists.');
        return;
      }

      const jobId = uuid();
      this._newJob(jobId);
      this.jobs[jobId].endpoint = endpoint;
      this.jobs[jobId].status = JOB_STATUS.NOT_CONNECTED;
      await this.selectJobId(jobId);

      try {
        this.prepareWebSocket(jobId);
      } catch (e) {
        await this.$nextTick();
        this.$notify.error('Invalid WebSocket endpoint.')
        this.deleteJobId(jobId);
      }
    },

    /**
     * Try to connect WebSocket.
     * @param jobId id of job.
     */
    prepareWebSocket(jobId) {
      if (!process.browser) {
        return;
      }

      const job = this.jobs[jobId];

      if (job.ws && job.ws.readyState !== WebSocket.CLOSED) {
        return;
      }

      job.ws = new WebSocket(job.endpoint);
      job.status = JOB_STATUS.CONNECTING;

      job.ws.onopen = () => {
        job.status = JOB_STATUS.RUNNING;
      };

      job.ws.onmessage = (event) => {
        let parsedData;
        try {
          parsedData = JSON.parse(event.data);
        } catch (e) {
          console.warn('Non-JSON data received: ' + jobId);
          return;
        }

        // pass to metric handling logic
        this.processMetric(parsedData, jobId);
      };

      job.ws.onclose = () => {
        if (job.ws) {
          job.ws = undefined;
          if (job.status === JOB_STATUS.CONNECTING) {
            job.status = JOB_STATUS.NOT_CONNECTED;
          }
        }
      };

      window.onbeforeunload = () => {
        if (job.ws) {
          job.ws.close();
        }
      };
    },

    /**
     * Process metric chunk or individual metric.
     * @param metric metric object.
     * @param jobId id of job.
     */
    async processMetric(metric, jobId) {
      // specific event broadcast
      if ('metricType' in metric) {
        await this.processIndividualMetric(metric, jobId);
      } else {
        // the first big metric chunk
        Object.keys(metric).forEach(metricType => {
          Object.values(metric[metricType]).forEach(async chunk => {
            await this.processIndividualMetric({
              metricType: metricType,
              data: chunk.data,
            }, jobId);
          });
        });
      }
    },

    /**
     * Process individual metric. All metric should be passed
     * through this method to be processed properly and propagated to
     * other components.
     * @param metricType type of metric.
     * @param data metric data.
     * @param jobId id of job.
     */
    async processIndividualMetric({ metricType, data }, jobId) {
      const job = this.jobs[jobId];

      let newItem = { group: metricType };
      // overwrite item object with received data
      Object.assign(newItem, data);

      data.stateTransitionEvents
        .filter(event => event.prevState != null)
        .forEach(event => {
          const { prevState, newState, timestamp } = event;

          let metricObj = {
            jobId,
            metricId: data.id,
            metricType,
            prevState,
            newState,
          };

          this.$eventBus.$emit('state-change-event', metricObj);
          this._cacheStageState(metricObj);

          if (metricType === 'JobMetric') {
            // READY -> EXECUTING -> COMPLETE / FAILED
            switch (prevState) {
              case STATE.READY:
                newItem.start = new Date(timestamp);
                break;
            }
            switch (newState) {
              case STATE.COMPLETE:
                job.status = JOB_STATUS.COMPLETE;
                newItem.end = new Date(timestamp);
                break;
              case STATE.FAILED:
                job.status = JOB_STATUS.FAILED;
                newItem.end = new Date(timestamp);
                break;
            }
          } else if (metricType === 'StageMetric') {
            // INCOMPLETE -> COMPLETE
            switch (newState) {
              case STATE.COMPLETE:
              case STATE.FAILED:
                // Stage does not have READY, so it cannot be
                // represented as a range of timeline.
                // So the only needed field is `start`.
                newItem.start = new Date(timestamp);
                break;
            }
          } else if (metricType === 'TaskMetric') {
            // READY -> EXECUTING -> (SHOULD_RETRY) -> COMPLETE / FAILED
            switch (prevState) {
              case STATE.READY:
                newItem.start = new Date(timestamp);
                break;
            }

            switch (newState) {
              case STATE.COMPLETE:
              case STATE.FAILED:
                newItem.end = new Date(timestamp);
                break;
            }
          }

          newItem.content = data.id;
        });

      // if data contains `dag`, it will send it to DAG component
      if (data.dag) {
        job.dag = data.dag;
        this.$eventBus.$emit('dag', {
          dag: data.dag,
          jobId: jobId,
          init: false,
          states: job.dagStageState,
        });
        this.buildMetricLookupMapWithDAG(jobId);
      }
      newItem.metricId = data.id;

      let prevItem = job.metricDataSet.get(newItem.id);
      if (!prevItem) {
        try {
          this.$eventBus.$emit('add-timeline-item', {
            jobId,
            item: newItem,
          });
          job.metricDataSet.add(newItem);
          this.addMetricToMetricLookupMap(newItem, jobId);
        } catch (e) {
          console.warn('Error when adding new item');
        }
        if (job.metricDataSet.length === 1) {
          this.moveTimeline(newItem.start, jobId);
        } else {
          this.fitTimeline(jobId);
        }
      } else {
        try {
          this.$eventBus.$emit('update-timeline-item', {
            jobId,
            item: newItem,
          });
          job.metricDataSet.update(newItem);
          this.addMetricToMetricLookupMap(newItem, jobId);
        } catch (e) {
          console.warn('Error when updating item');
        }
        if (!(prevItem.start === newItem.start && prevItem.end === newItem.end)) {
          this.fitTimeline(jobId);
        }
      }
    },

    _cacheStageState({ jobId, metricId, metricType, newState }) {
      if (metricType !== 'StageMetric') {
        return;
      }

      const job = this.jobs[jobId];
      job.dagStageState[metricId] = newState;
    },

    /**
     * Send fit-timeline event.
     * @param jobId id of job.
     */
    fitTimeline(jobId) {
      this.$eventBus.$emit('fit-timeline', jobId);
    },

    /**
     * Send move-timeline event.
     * @param time Date or timestamp to move timeline.
     * @param jobId id of job.
     */
    moveTimeline(time, jobId) {
      this.$eventBus.$emit('move-timeline', {
        jobId: jobId,
        time: time,
      });
    },

    _flatten(metric) {
      let newMetric = {};
      Object.keys(metric).forEach(key => {
        if (key === 'properties') {
          Object.assign(newMetric, this._flatten(metric[key]));
        } else if (key !== 'irDag') {
          newMetric[key] = metric[key];
        }
      });

      return newMetric;
    },

    /**
     * Build metricLookupMap based on DAG.
     * @param jobId id of job.
     */
    buildMetricLookupMapWithDAG(jobId) {
      const job = this.jobs[jobId];
      job.dag.vertices.forEach(stage => {
        Vue.set(job.metricLookupMap, stage.id, this._flatten(stage));
        stage.properties.irDag.vertices.forEach(vertex => {
          Vue.set(job.metricLookupMap, vertex.id, this._flatten(vertex));
        });
        stage.properties.irDag.edges.forEach(edge => {
          const edgeId = edge.properties.runtimeEdgeId;
          Vue.set(job.metricLookupMap, edgeId, this._flatten(edge));
        });
      });
      job.dag.edges.forEach(edge => {
        const edgeId = edge.properties.runtimeEdgeId;
        Vue.set(job.metricLookupMap, edgeId, this._flatten(edge));
      });
    },

    /**
     * Add a metric data to its metricLookupMap.
     * @param metric metric data.
     * @param jobId id of job.
     */
    addMetricToMetricLookupMap(metric, jobId) {
      const job = this.jobs[jobId];
      if (metric.group === 'JobMetric') {
        Vue.set(job.metricLookupMap, metric.id, metric);
      } else if (metric.group === 'TaskMetric') {
        Vue.set(job.metricLookupMap, metric.id, metric);
      }
      this.$eventBus.$emit('build-table-data', {
        metricId: metric.id,
        jobId: jobId,
      });
    },
  },
}
</script>

<style>
.job-table {
  margin-top: 20px;
}

.dialog-card {
  margin-bottom: 20px;
}

.dialog-row {
  padding: 10px;
}
</style>
