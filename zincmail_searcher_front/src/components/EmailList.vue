<template>
  <div
    className="flex flex-col bg-dark-500 w-6/12 mr-1 px-2 rounded-3xl h-full"
  >
    <div className="px-3 pb-1 pt-3">
      <span className="text-light-200 text-lg pr-5">Results</span>
      <font-awesome-icon
        :icon="['fass', 'caret-down']"
        style="color: #ecebf0"
        size="lg"
      />
    </div>
    <div className="flex flex-col px-2 pb-5 overflow-y-auto hide-scrollbar">
      <div
        v-for="email in emails"
        :key="email.id"
        v-on:click="selectEmail(email)"
      >
        <EmailCard :email="email" />
      </div>
    </div>
  </div>
</template>

<script lang="ts">
import { defineComponent } from "vue";
import EmailCard from "./EmailCard.vue";
import { useStore } from "vuex";
import { computed } from "vue";
import { State, Email } from "./store";

export default defineComponent({
  name: "EmailList",
  components: {
    EmailCard,
  },
  setup() {
    const store = useStore<State>();
    let emails = computed<Email[]>(() => store.getters.emailsList);

    function selectEmail(selectedEmail: Email) {
      store.dispatch("selectEmail", selectedEmail);
    }

    return {
      emails,
      selectEmail,
    };
  },
  methods: {},
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
