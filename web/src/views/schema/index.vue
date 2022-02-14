<template>
  <div class="tab-container">
    <el-row v-if="data">
    <el-tabs v-model="activeTab" style="margin-top:15px;" type="border-card">
      <el-tab-pane v-for="item in data" :key="item.key" :label="item.ctype" :name="item.ctype">
				<el-table
					:data="item.data"
					style="width: 100%">
					<el-table-column v-for="col in item.columns"
						:prop="col"
						:label="col"
					  :width="(col == 'desc') ? 800: col.includes('table') ? 120 : col.includes('column')? 350 : 200 + 'px'"
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
  name: 'Schema',
  data() {
    return {
      pid: "483248201",
			data: null,
      activeTab: null,
    }
  },
  watch: {
    activeName(val) {
      this.$router.push(`${this.$route.path}?tab=${val}`)
    }
  },
  created() {
			request({url: '/api/meta/',
            method: 'get'}).then(resp => {
				this.data = resp.meta
        this.activeTab = 'den'
			})
  },
  methods: {
  }
}
</script>

<style scoped>
  .tab-container {
    margin: 30px;
  }
</style>
