import { createStore } from "vuex";

export interface Email {
  id: number;
  Date: Date;
  From: string;
  To: string[];
  Subject: string;
  Body: string;
  isSelected: boolean;
}

export interface State {
  emailsList: Email[];
}

//export const key: InjectionKey<Store<State>> = Symbol();

export default createStore<State>({
  state: {
    emailsList: [],
  },
  mutations: {
    updateEmailsList(state, emailsListUpdated: Email[]) {
      state.emailsList = emailsListUpdated;
    },
    selectEmail(state, selectedEmail: Email) {
      state.emailsList.forEach((email) => {
        if (email === selectedEmail) {
          email.isSelected = true;
        } else {
          email.isSelected = false;
        }
      });
    },
  },
  actions: {
    updateEmailsList({ commit }, emailsListUpdated) {
      commit("updateEmailsList", emailsListUpdated);
    },
    selectEmail({ commit }, selectedEmail: Email) {
      commit("selectEmail", selectedEmail);
    },
  },
  getters: {
    emailsList: (state) => state.emailsList,
  },
});
