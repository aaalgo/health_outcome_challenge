<template>
  <div class="dashboard-editor-container" v-if="data">

    <el-row>
      Select Time Frame: <el-select v-model="year" placeholder="Select" :value="'all'">
          <el-option v-for="year in years" :key="year" :label="year" :value="year"> </el-option>
        </el-select>
    </el-row>

    <panel-group :counts='data[year]["counts"]' />

    <el-row style="background:#fff;padding:16px 16px 0;margin-bottom:32px;">
      <map-chart :chart-data='data[year]["counts"]["claims_by_state"]' />
    </el-row>


    <!--
    <el-row style="background:#fff;padding:16px 16px 0;margin-bottom:32px;">
      <line-chart :chart-data="data[year].lineChartData" />
    </el-row>
    -->

    <el-row :gutter="32">
      <el-col :xs="24" :sm="24" :lg="8">
        <div class="chart-wrapper">
          <pie-chart :chart-data='data[year]["counts"]["claims_by_ctype"]'/>
        </div>
      </el-col>
      <el-col :xs="24" :sm="24" :lg="8">
        <div class="chart-wrapper">
          <pie-chart2 :chart-data='data[year]["counts"]["ads_by_ctype"]'/>
        </div>
      </el-col>
      <el-col :xs="24" :sm="24" :lg="8">
        <div class="chart-wrapper">
          <bar-chart :chart-data='data[year]["counts"]["age"]'/>
        </div>
      </el-col>
    </el-row>

      <!--
    <el-row :gutter="8">
      <el-col :xs="{span: 24}" :sm="{span: 24}" :md="{span: 24}" :lg="{span: 12}" :xl="{span: 12}" style="padding-right:8px;margin-bottom:30px;">
        <transaction-table />
      </el-col>
      <el-col :xs="{span: 24}" :sm="{span: 12}" :md="{span: 12}" :lg="{span: 6}" :xl="{span: 6}" style="margin-bottom:30px;">
        <todo-list />
      </el-col>
      <el-col :xs="{span: 24}" :sm="{span: 12}" :md="{span: 12}" :lg="{span: 6}" :xl="{span: 6}" style="margin-bottom:30px;">
        <box-card />
      </el-col>
    </el-row>
      -->
  </div>
</template>

<script>
import request from '@/utils/request'
import PanelGroup from './components/PanelGroup'
import LineChart from './components/LineChart'
import MapChart from './components/MapChart'
import RaddarChart from './components/RaddarChart'
import PieChart from './components/PieChart'
import PieChart2 from './components/PieChart2'
import BarChart from './components/BarChart'
//import TransactionTable from './components/TransactionTable'
//import TodoList from './components/TodoList'
//import BoxCard from './components/BoxCard'

const lineChartData = {
  newVisitis: {
    expectedData: [100, 120, 161, 134, 105, 160, 165],
    actualData: [120, 82, 91, 154, 162, 140, 145]
  },
  messages: {
    expectedData: [200, 192, 120, 144, 160, 130, 140],
    actualData: [180, 160, 151, 106, 145, 150, 130]
  },
  purchases: {
    expectedData: [80, 100, 121, 104, 105, 90, 100],
    actualData: [120, 90, 100, 138, 142, 130, 130]
  },
  shoppings: {
    expectedData: [130, 140, 141, 142, 145, 150, 160],
    actualData: [120, 82, 91, 154, 162, 140, 130]
  }
}

export default {
  name: 'DashboardAdmin',
  components: {
    PanelGroup,
    LineChart,
    RaddarChart,
    PieChart,
    PieChart2,
    BarChart,
    MapChart,
    //TransactionTable,
    //TodoList,
    //BoxCard
  },
  data() {
    return {
      years: ['2009-2011', '2011', '2010', '2009'],
      year: '2009-2011',
      data: null,
      /*
      counts: {
        'patients': 2000,
        'providers': 4000,
        'claims': 8000,
        'amount': 12000
      },
      lineChartData: lineChartData.newVisitis
      */
    }
  },
  created () {
    request({url: '/api/stats/',
          method: 'get'}).then(resp => {
      this.data = resp.data
    })
  },
  methods: {
    handleSetLineChartData(type) {
      //this.lineChartData = lineChartData[type]
    }
  }
}
</script>

<style lang="scss" scoped>
.dashboard-editor-container {
  padding: 32px;
  background-color: rgb(240, 242, 245);
  position: relative;

  .chart-wrapper {
    background: #fff;
    padding: 16px 16px 0;
    margin-bottom: 32px;
  }
}

@media (max-width:1024px) {
  .chart-wrapper {
    padding: 8px;
  }
}
</style>
