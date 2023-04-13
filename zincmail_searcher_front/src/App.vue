<template>
  <div
    className="h-screen w-screen flex justify-center items-center bg-slate-500"
  >
    <MainLayout />
  </div>
</template>

<script lang="ts">
import { defineComponent } from "vue";
import MainLayout from "./components/MainLayout.vue";

export default defineComponent({
  name: "App",
  components: {
    MainLayout,
  },
});
</script>

<style src="./style.css"></style>
