<template>
  <div
    className="flex flex-col bg-dark-500 w-6/12 ml-1 px-2 rounded-3xl overflow-y-auto hide-scrollbar"
  >
    <span className="px-5 text-xs text-light-100 font-light mt-6">{{
      selectedEmail != undefined ? dateToString(selectedEmail.Date) : ""
    }}</span>
    <span className="px-5 text-lg text-light-100 font-bold mb-6">{{
      selectedEmail != undefined ? selectedEmail.Subject : ""
    }}</span>
    <span className="px-5 text-sm text-light-100 font-light mb-2">
      {{
        selectedEmail != undefined ? "from: " + selectedEmail.From : ""
      }}</span
    >
    <span className="px-5 text-sm text-light-100 font-light mb-2 break-words">
      {{
        selectedEmail != undefined
          ? "to: " + listToString(selectedEmail.To)
          : ""
      }}</span
    >
    <span
      className="pl-7 pr-5 text-xs text-light-500 break-words my-6 whitespace-pre-line"
    >
      {{ selectedEmail != undefined ? selectedEmail.Body : "" }}
    </span>
  </div>
</template>

<script lang="ts">
import { defineComponent } from "vue";
import { computed } from "vue";
import { useStore } from "vuex";
import { Email } from "./store";
import { State } from "./store";

export default defineComponent({
  name: "EmailDetails",
  setup() {
    const store = useStore<State>();
    //let emails = computed<Email[]>(() => store.getters.emailsList);
    let selectedEmail = computed<Email>(() =>
      store.getters.emailsList.find((email: Email) => email.isSelected === true)
    );

    return {
      selectedEmail,
    };
  },
  methods: {
    dateToString(date: Date) {
      var dateString =
        date.getFullYear().toString().padStart(4, "0") +
        "/" +
        (date.getMonth() + 1).toString().padStart(2, "0") +
        "/" +
        date.getDate().toString().padStart(2, "0") +
        " " +
        date.getHours().toString().padStart(2, "0") +
        ":" +
        date.getMinutes().toString().padStart(2, "0");
      return dateString;
    },
    listToString(list: string[]): string {
      let result = "";
      for (let i = 0; i < list.length - 1; i++) {
        result += list[i] + ", ";
      }
      result += list[list.length - 1];
      return result;
    },
  },
});
</script>
<!-- Add "scoped" attribute to limit CSS to this component only -->
<style scoped>
.overflow-y-auto {
  overflow-y: auto !important;
}

.hide-scrollbar:hover::-webkit-scrollbar {
  width: 0.15em;
}
.hide-scrollbar:hover::-webkit-scrollbar-thumb {
  background-color: #7a7d89;
}

.hide-scrollbar::-webkit-scrollbar {
  background-color: #383b47;
  width: 0.15em;
}

.hide-scrollbar::-webkit-scrollbar-thumb {
  background-color: #383b47;
}
</style>
