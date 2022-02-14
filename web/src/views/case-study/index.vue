<template>
  <div class="tab-container">
    <el-row>
    <el-col :span="12">
      Beneficieary ID: 
			<el-autocomplete
				class="inline-input"
				v-model="pid"
				:fetch-suggestions="suggest"
				placeholder="Please Input"
			></el-autocomplete>
			<el-button type="primary" @click="load()">Load</el-button>
  	</el-col>
    </el-row>
    <el-row v-if="meta">
    <el-tabs v-model="activeTab" style="margin-top:15px;" type="border-card">
      <el-tab-pane v-for="item in meta" :key="item.key" :label="item.label" :name="item.label">
				<el-table
					:data="data[item.label]"
          height="2500"
					style="width: 100%"
					:row-class-name="tableRowClassName">
					<el-table-column v-for="col in item.columns"
						:prop="col"
						:label="col"
					>
					</el-table-column>
				</el-table>
      </el-tab-pane>
    </el-tabs>
    </el-row>
  </div>
</template>

<script>
import request from '@/utils/request'

export default {
  name: 'CaseStudy',
  data() {
    return {
      pid: "test",
			meta: null,
			data: null,
      empty: [],
      activeTab: null,
    }
  },
  watch: {
    activeName(val) {
      this.$router.push(`${this.$route.path}?tab=${val}`)
    }
  },
  created() {
    // init the default selected tab
    const tab = this.$route.query.tab
    if (tab) {
      this.activeName = tab
    }
  },
  methods: {

		tableRowClassName ({row, rowIndex}) {
			if (rowIndex % 2 === 1) {
				return 'odd-row'
			} else {
				return 'even-row'
			}
		},

		suggest (query, cb) {
			cb([
{"value": "100058963"},
{"value": "109434825"},
{"value": "100260517"},
{"value": "102837081"},
{"value": "116822101"},
{"value": "110869047"},
{"value": "116675097"},
{"value": "108711981"},
{"value": "117463343"},
{"value": "107981497"},
			])
	  },

		load () {
			request({url: '/api/case/',
							 method: 'get',
							 params: {pid: this.pid}}).then(resp => {
				this.meta = resp.meta
				this.data = resp.data	
        this.activeTab = 'den'
			})
		}
  }
}
</script>

<style scoped>
  .tab-container {
    margin: 30px;
  }

  .el-table .even-row {
    background: #f0f9eb;
  }
</style>
