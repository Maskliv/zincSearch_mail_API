<template>
  <div
    className="bg-dark-500 flex flex-row items-center py-6 px-8 mb-2 ml-2 mr-2 mt-2 rounded-3xl"
  >
    <form className="w-full" @submit.prevent="search(input)">
      <input
        placeholder="Search..."
        className="w-96per mr-auto ml-4 bg-transparent outline-none text-light-300"
        v-model="input"
      />
      <button>
        <font-awesome-icon
          :icon="['fass', 'circle-arrow-right']"
          style="color: #d5d6df"
          size="2xl"
        />
      </button>
    </form>
  </div>
</template>

<script lang="ts">
import { computed, defineComponent } from "vue";
import axios from "axios";
import { useStore } from "vuex";
import { State } from "./store";
import { Email } from "./store";

const URL = "/search/";

export default defineComponent({
  name: "ContentHeader",
  data() {
    return {
      input: "",
    };
  },
  setup() {
    const store = useStore<State>();
    const emails = computed<Email[]>(() => store.getters.emailsList);
    function updateEmailsList(emailsList: Email[]) {
      store.dispatch("updateEmailsList", emailsList);
    }
    async function search(input: string) {
      const response = await axios.get(URL + input);
      console.log(response.data);
      //console.log(response.toString);
      //console.log(typeof response.data);
      if (response.data) {
        // eslint-disable-next-line
        const emailsList: Email[] = response.data.map((obj: any) => {
          const email: Email = {
            id: obj.Id,
            Date: new Date(Date.parse(obj.Date)),
            From: obj.From,
            To: obj.To,
            Subject: obj.Subject,
            Body: obj.Body,
            isSelected: false,
          };
          return email;
        });
        updateEmailsList(emailsList);
      }
      console.log(emails);
    }

    return {
      emails,
      updateEmailsList,
      search,
    };
  },
  methods: {},
});
</script>

<!-- Add "scoped" attribute to limit CSS to this component only -->
<style scoped>
.w-96per {
  width: 96%;
}
</style>
