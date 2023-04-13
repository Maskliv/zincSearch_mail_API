<template>
  <div className="w-10/12 flex flex-col h-[95vh] bg-slate-600 rounded-3xl">
    <ZincMailBanner />
    <div className="flex flex-col w-full bg-dark-600 h-[83vh] rounded-3xl">
      <ContentHeader />
      <div className="flex flex-row px-2 pb-2 flex-1 overflow-y-auto">
        <EmailList />
        <EmailDetails />
      </div>
    </div>
  </div>
</template>

<script lang="ts">
import { defineComponent } from "vue";
import ContentHeader from "./ContentHeader.vue";
import EmailList from "./EmailList.vue";
import EmailDetails from "./EmailDetails.vue";
import ZincMailBanner from "./ZincMailBanner.vue";

export default defineComponent({
  name: "MainLayout",
  components: {
    ContentHeader,
    EmailList,
    EmailDetails,
    ZincMailBanner,
  },
});
</script>

<!-- Add "scoped" attribute to limit CSS to this component only -->
<style scoped></style>
