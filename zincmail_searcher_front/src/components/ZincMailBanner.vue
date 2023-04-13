<template>
  <div className="flex flex-row justify-center items-center h-[12vh]">
    <img src="../assets/logo3.png" className="h-full py-3 " />
    <span className="font-varela font-bold text-2xl pl-10 text-light-300">
      ZincMail Searcher
    </span>
  </div>
</template>

<script lang="ts">
import { defineComponent } from "vue";

export default defineComponent({
  name: "ZincMailBanner",
});
</script>

<!-- Add "scoped" attribute to limit CSS to this component only -->
<style scoped>
@import url("https://fonts.googleapis.com/css2?family=Varela+Round&display=swap");
.font-varela {
  font-family: "Varela Round", sans-serif;
}
</style>
