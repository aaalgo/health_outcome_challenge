<template>
  <el-row :gutter="40" class="panel-group">
    <el-col :xs="9" :sm="9" :lg="4" class="card-panel-col">
      <div class="card-panel">
        <div class="card-panel-icon-wrapper icon-users">
          <font-awesome-icon icon="users" />
        </div>
        <div class="card-panel-description">
          <div class="card-panel-text">
            Beneficiaries
          </div>
          <count-to :start-val="0" :end-val="counts['patients']" :duration="2000" class="card-panel-num" />
        </div>
      </div>
    </el-col>
    <el-col :xs="9" :sm="9" :lg="4" class="card-panel-col">
      <div class="card-panel">
        <div class="card-panel-icon-wrapper icon-money">
          <font-awesome-icon icon="dollar-sign" />
        </div>
        <div class="card-panel-description">
          <div class="card-panel-text">
            Payment Amount
          </div>
          <count-to :start-val="0" :end-val="counts['amount']" :duration="2000" class="card-panel-num" />
        </div>
      </div>
    </el-col>
    <el-col :xs="9" :sm="9" :lg="4" class="card-panel-col">
      <div class="card-panel">
        <div class="card-panel-icon-wrapper icon-claims">
          <svg-icon icon-class="message" class-name="card-panel-icon" />
        </div>
        <div class="card-panel-description">
          <div class="card-panel-text">
            Claims
          </div>
          <count-to :start-val="0" :end-val="counts['claims']" :duration="2000" class="card-panel-num" />
        </div>
      </div>
    </el-col>
    <el-col :xs="9" :sm="9" :lg="4" class="card-panel-col">
      <div class="card-panel">
        <div class="card-panel-icon-wrapper icon-providers">
          <font-awesome-icon icon="ad" />
        </div>
        <div class="card-panel-description">
          <div class="card-panel-text">
            By ICD
          </div>
          <count-to :start-val="0" :end-val="counts['adverse_dgns_e']" :duration="2000" class="card-panel-num" />
        </div>
      </div>
    </el-col>
    <el-col :xs="9" :sm="9" :lg="4" class="card-panel-col">
      <div class="card-panel">
        <div class="card-panel-icon-wrapper icon-providers">
          <font-awesome-icon icon="ad" />
        </div>
        <div class="card-panel-description">
          <div class="card-panel-text">
            By POA
          </div>
          <count-to :start-val="0" :end-val="counts['adverse_poa']" :duration="2000" class="card-panel-num" />
        </div>
      </div>
    </el-col>
    <el-col :xs="9" :sm="9" :lg="4" class="card-panel-col">
      <div class="card-panel">
        <div class="card-panel-icon-wrapper icon-providers">
          <font-awesome-icon icon="ad" />
        </div>
        <div class="card-panel-description">
          <div class="card-panel-text">
            By Date
          </div>
          <!--
          <count-to :start-val="0" :end-val="counts['adverse_poa']" :duration="2000" class="card-panel-num" />
          -->
        </div>
      </div>
    </el-col>
    <!--
    <el-col :xs="12" :sm="12" :lg="6" class="card-panel-col">
      <div class="card-panel" @click="handleSetLineChartData('shoppings')">
        <div class="card-panel-icon-wrapper icon-shopping">
          <svg-icon icon-class="shopping" class-name="card-panel-icon" />
        </div>
        <div class="card-panel-description">
          <div class="card-panel-text">
            Shoppings
          </div>
          <count-to :start-val="0" :end-val="13600" :duration="3600" class="card-panel-num" />
        </div>
      </div>
    </el-col>
    -->
  </el-row>
</template>

<script>
import CountTo from 'vue-count-to'

export default {
  props: ['counts'],
  components: {
    CountTo
  },
}
</script>

<style lang="scss" scoped>
.panel-group {
  margin-top: 18px;

  .card-panel-col {
    margin-bottom: 32px;
  }

  .card-panel {
    height: 108px;
    cursor: pointer;
    font-size: 12px;
    position: relative;
    overflow: hidden;
    color: #666;
    background: #fff;
    box-shadow: 4px 4px 40px rgba(0, 0, 0, .05);
    border-color: rgba(0, 0, 0, .05);

    &:hover {
      .card-panel-icon-wrapper {
        color: #fff;
      }

      .icon-users {
        background: #40c9c6;
      }

      .icon-providers {
        background: #36a3f7;
      }

      .icon-claims {
        background: #34bfa3;
      }

      .icon-money {
        background: #f4516c;
      }

    }

    .icon-users {
      color: #40c9c6;
    }

    .icon-providers {
      color: #36a3f7;
    }

    .icon-money {
      color: #f4516c;
    }

    .icon-claims {
      color: #34bfa3
    }

    .card-panel-icon-wrapper {
      float: left;
      margin: 14px 0 0 14px;
      padding: 16px;
      transition: all 0.38s ease-out;
      border-radius: 6px;
      font-size: 48px;
    }

    .card-panel-icon {
      float: left;
    }

    .card-panel-description {
      float: right;
      font-weight: bold;
      margin: 26px;
      margin-left: 0px;

      .card-panel-text {
        line-height: 18px;
        color: rgba(0, 0, 0, 0.45);
        font-size: 16px;
        margin-bottom: 12px;
      }

      .card-panel-num {
        font-size: 20px;
      }
    }
  }
}

@media (max-width:550px) {
  .card-panel-description {
    display: none;
  }

  .card-panel-icon-wrapper {
    float: none !important;
    width: 100%;
    height: 100%;
    margin: 0 !important;

    .svg-icon {
      display: block;
      margin: 14px auto !important;
      float: none !important;
    }
  }
}
</style>
